/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package controller

import (
	"fmt"
	"time"

	"github.com/golang/glog"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"

	riffv1alpha1 "github.com/projectriff/riff/kubernetes-crds/pkg/apis/projectriff.io/v1alpha1"
	clientset "github.com/projectriff/riff/kubernetes-crds/pkg/client/clientset/versioned"
	riffscheme "github.com/projectriff/riff/kubernetes-crds/pkg/client/clientset/versioned/scheme"
	"log"
	"github.com/projectriff/riff/message-transport/pkg/transport"
	"github.com/projectriff/riff/function-sidecar/pkg/carrier"
	"github.com/projectriff/riff/function-sidecar/pkg/dispatcher/grpc"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	informers "k8s.io/client-go/informers/core/v1"
	listers "k8s.io/client-go/listers/core/v1"
	"github.com/projectriff/riff/kubernetes-crds/pkg/client/listers/projectriff.io/v1alpha1"
	v1alpha12 "github.com/projectriff/riff/kubernetes-crds/pkg/client/informers/externalversions/projectriff.io/v1alpha1"
	"k8s.io/apimachinery/pkg/api/meta"
	"io"
	"github.com/projectriff/riff/function-sidecar/pkg/dispatcher"
)

const controllerAgentName = "stream-gateway-controller"

const (
	// SuccessSynced is used as part of the Event 'reason' when a Foo is synced
	SuccessSynced = "Synced"
	// MessageResourceSynced is the message used for an Event fired when a Foo
	// is synced successfully
	MessageResourceSynced = "Foo synced successfully"
)

// ctrl is the controller implementation for Foo resources
type ctrl struct {
	// kubeclientset is a standard kubernetes clientset
	kubeclientset kubernetes.Interface
	// sampleclientset is a clientset for our own API group
	riffclientset clientset.Interface

	endpointsLister listers.EndpointsLister
	endpointsSynced cache.InformerSynced

	linksLister v1alpha1.LinkLister

	// workqueue is a rate limited work queue. This is used to queue work to be
	// processed instead of performing it as soon as a change happens. This
	// means we can ensure we only process a fixed amount of resources at a
	// time, and makes it easy to ensure we are never processing the same item
	// simultaneously in two different workers.
	workqueue workqueue.RateLimitingInterface
	// recorder is an event recorder for recording Event resources to the
	// Kubernetes API.
	recorder record.EventRecorder

	consumerFactory transport.ConsumerFactory
	producer        transport.Producer
	carriers        map[string]*registration
}

type registration struct {
	producer   transport.Producer
	consumer   transport.Consumer
	dispatcher dispatcher.Dispatcher
}

// NewController returns a new sample controller
func NewController(
	kubeclientset kubernetes.Interface,
	riffclientset clientset.Interface,
	endpointsInformer informers.EndpointsInformer,
	linksInformer v1alpha12.LinkInformer,
	consumerFactory transport.ConsumerFactory,
	producer transport.Producer,
) *ctrl {

	// Create event broadcaster
	// Add sample-controller types to the default Kubernetes Scheme so Events can be
	// logged for sample-controller types.
	riffscheme.AddToScheme(scheme.Scheme)
	glog.V(4).Info("Creating event broadcaster")
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(glog.Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeclientset.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: controllerAgentName})

	controller := &ctrl{
		kubeclientset:   kubeclientset,
		riffclientset:   riffclientset,
		endpointsLister: endpointsInformer.Lister(),
		endpointsSynced: endpointsInformer.Informer().HasSynced,
		linksLister:     linksInformer.Lister(),
		workqueue:       workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "Endpoints"),
		recorder:        recorder,
		consumerFactory: consumerFactory,
		producer:        producer,
		carriers:        make(map[string]*registration),
	}

	glog.Info("Setting up event handlers")
	// Set up an event handler for when Link resources change
	endpointsInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueueEndpoint,
		UpdateFunc: func(old, new interface{}) {
			newEndpoints := new.(*corev1.Endpoints)
			oldEndpoints := old.(*corev1.Endpoints)
			if newEndpoints.ResourceVersion == oldEndpoints.ResourceVersion {
				return
			}
			controller.enqueueEndpoint(new)
		},
	})

	return controller
}

// Run will set up the event handlers for types we are interested in, as well
// as syncing informer caches and starting workers. It will block until stopCh
// is closed, at which point it will shutdown the workqueue and wait for
// workers to finish processing their current work items.
func (c *ctrl) Run(threadiness int, stopCh <-chan struct{}) error {
	defer runtime.HandleCrash()
	defer c.workqueue.ShutDown()

	// Start the informer factories to begin populating the informer caches
	glog.Info("Starting stream gateway controller")

	// Wait for the caches to be synced before starting workers
	glog.Info("Waiting for informer caches to sync")
	if ok := cache.WaitForCacheSync(stopCh, c.endpointsSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	glog.Info("Starting workers")
	// Launch two workers to process Foo resources
	for i := 0; i < threadiness; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	glog.Info("Started workers")
	<-stopCh
	glog.Info("Shutting down workers")

	return nil
}

// runWorker is a long-running function that will continually call the
// processNextWorkItem function in order to read and process a message on the
// workqueue.
func (c *ctrl) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem will read a single work item off the workqueue and
// attempt to process it, by calling the syncHandler.
func (c *ctrl) processNextWorkItem() bool {
	obj, shutdown := c.workqueue.Get()

	if shutdown {
		return false
	}

	// We wrap this block in a func so we can defer c.workqueue.Done.
	err := func(obj interface{}) error {
		// We call Done here so the workqueue knows we have finished
		// processing this item. We also must remember to call Forget if we
		// do not want this work item being re-queued. For example, we do
		// not call Forget if a transient error occurs, instead the item is
		// put back on the workqueue and attempted again after a back-off
		// period.
		defer c.workqueue.Done(obj)
		var key string
		var ok bool
		// We expect strings to come off the workqueue. These are of the
		// form namespace/name. We do this as the delayed nature of the
		// workqueue means the items in the informer cache may actually be
		// more up to date that when the item was initially put onto the
		// workqueue.
		if key, ok = obj.(string); !ok {
			// As the item in the workqueue is actually invalid, we call
			// Forget here else we'd go into a loop of attempting to
			// process a work item that is invalid.
			c.workqueue.Forget(obj)
			runtime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}
		// Run the syncHandler, passing it the namespace/name string of the
		// Endpoint resource to be synced.
		if err := c.syncHandler(key); err != nil {
			return fmt.Errorf("error syncing '%s': %s", key, err.Error())
		}
		// Finally, if no error occurs we Forget this item so it does not
		// get queued again until another change happens.
		c.workqueue.Forget(obj)
		glog.Infof("Successfully synced '%s'", key)
		return nil
	}(obj)

	if err != nil {
		runtime.HandleError(err)
		return true
	}

	return true
}

// TODO: syncHandler compares the actual state with the desired, and attempts to
// converge the two. It then updates the Status block of the Foo resource
// with the current status of the resource.
func (c *ctrl) syncHandler(key string) error {
	log.Printf("Looking at %v\n", key)
	// Convert the namespace/name string into a distinct namespace and name
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		runtime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}

	// Get the Endpoint resource with this namespace/name
	endpoint, err := c.endpointsLister.Endpoints(namespace).Get(name)
	if err != nil {
		// The Endpoint resource may no longer exist, in which case we stop
		// processing.
		if errors.IsNotFound(err) {
			runtime.HandleError(fmt.Errorf("foo '%s' in work queue no longer exists", key))
			// TODO: stop gRPC clients
			return nil
		}

		return err
	}

	ready := false
	for _, s := range endpoint.Subsets {
		if len(s.Addresses) > 0 {
			ready = true
			break
		}
	}

	reg, ok := c.carriers[key]
	if !ok && !ready {
		log.Printf("Waiting for endpoint %v to become ready\n", key)
	} else if !ok && ready {
		link, err := c.linksLister.Links(namespace).Get(name)
		if err != nil {
			// TODO
			log.Printf("Error getting link: %v\n", err)
		}
		consumer, err := c.consumerFactory(link.Spec.Input, /*key*/ name)
		if err != nil {
			log.Printf("Error creating consumer %v\n", err)
		} else {

			service, err := c.kubeclientset.CoreV1().Services(namespace).Get(name, v1.GetOptions{})
			if err != nil {
				log.Printf("Error looking up service: %v\n", err)

			} else {
				port := -1
				for _, p := range service.Spec.Ports {
					if p.Name == "grpc" {
						port = int(p.Port)
						break
					}
				}
				hostname := fmt.Sprintf("%s.%s", name, namespace)
				log.Printf("Creating dispatcher to %v:%v\n", hostname, port)
				dispatcher, err := grpc.NewGrpcDispatcher(hostname, port, link.Spec.Windowing, 60*time.Second) //TODO: use link.fn.protocol
				if err != nil {
					log.Printf("Error creating dispatcher: %v\n", err)
				} else {
					output := link.Spec.Output
					if output == "" {
						output = "replies"
					}
					carrier.Run(consumer, c.producer, dispatcher, output) // this spawns 2 goroutines
					c.carriers[key] = &registration{c.producer, consumer, dispatcher}

					c.recorder.Event(endpoint, corev1.EventTypeNormal, SuccessSynced, MessageResourceSynced)
				}
			}
			_ = reg
		}
	} else if ok && !ready {
		log.Printf("Shutting down %v\n", key)
		// TODO: shutdown
		if closer, ok := reg.consumer.(io.Closer); ok {
			closer.Close()
		}
		if closer, ok := reg.dispatcher.(io.Closer); ok {
			closer.Close()
		}
		delete(c.carriers, key)
		c.recorder.Event(endpoint, corev1.EventTypeNormal, "Not"+SuccessSynced, "Not "+MessageResourceSynced)
	} else if ok && ready {
		log.Printf("%v still active, moving on...\n", key)
	}
	return nil
}

func (c *ctrl) updateLinkStatus(link *riffv1alpha1.Link, deployment *appsv1.Deployment) error {
	// NEVER modify objects from the store. It's a read-only, local cache.
	// You can use DeepCopy() to make a deep copy of original object and modify this copy
	// Or create a copy manually for better performance
	linkCopy := link.DeepCopy()
	//TODO: linkCopy.Status.AvailableReplicas = deployment.Status.AvailableReplicas
	// If the CustomResourceSubresources feature gate is not enabled,
	// we must use Update instead of UpdateStatus to update the Status block of the Foo resource.
	// UpdateStatus will not allow changes to the Spec of the resource,
	// which is ideal for ensuring nothing other than resource status has been updated.
	_, err := c.riffclientset.ProjectriffV1alpha1().Links(link.Namespace).Update(linkCopy)
	return err
}

// enqueueEndpoint takes a Link resource and converts it into a namespace/name
// string which is then put onto the work queue. This method should *not* be
// passed resources of any type other than Link.
func (c *ctrl) enqueueEndpoint(obj interface{}) {
	var key string
	var err error
	a, err := meta.Accessor(obj)
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		runtime.HandleError(err)
		return
	}
	if _, ok := a.GetLabels()["link"]; !ok {
		//log.Printf("Skipping %v: %v\n", key, a.GetLabels())
		return
	}
	c.workqueue.AddRateLimited(key)
}
