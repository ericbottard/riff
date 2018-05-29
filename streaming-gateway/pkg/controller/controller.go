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
	informers "github.com/projectriff/riff/kubernetes-crds/pkg/client/informers/externalversions/projectriff.io/v1alpha1"
	listers "github.com/projectriff/riff/kubernetes-crds/pkg/client/listers/projectriff.io/v1alpha1"
	"log"
	"github.com/projectriff/riff/message-transport/pkg/transport"
	"github.com/projectriff/riff/function-sidecar/pkg/carrier"
	"github.com/projectriff/riff/function-sidecar/pkg/dispatcher/grpc"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
)

const controllerAgentName = "stream-gateway-controller"

const (
	// SuccessSynced is used as part of the Event 'reason' when a Foo is synced
	SuccessSynced = "Synced"
	// ErrResourceExists is used as part of the Event 'reason' when a Foo fails
	// to sync due to a Deployment of the same name already existing.
	ErrResourceExists = "ErrResourceExists"

	// MessageResourceExists is the message used for Events when a resource
	// fails to sync due to a Deployment already existing
	MessageResourceExists = "Resource %q already exists and is not managed by Foo"
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

	linksLister listers.LinkLister
	linksSynced cache.InformerSynced

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
	producer transport.Producer
	consumer transport.Consumer
}

// NewController returns a new sample controller
func NewController(
	kubeclientset kubernetes.Interface,
	riffclientset clientset.Interface,
	linkInformer informers.LinkInformer,
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
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeclientset.CoreV1().Events("default")})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: controllerAgentName})

	controller := &ctrl{
		kubeclientset:   kubeclientset,
		riffclientset:   riffclientset,
		linksLister:     linkInformer.Lister(),
		linksSynced:     linkInformer.Informer().HasSynced,
		workqueue:       workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "Links"),
		recorder:        recorder,
		consumerFactory: consumerFactory,
		producer:        producer,
		carriers:        make(map[string]*registration),
	}

	glog.Info("Setting up event handlers")
	// Set up an event handler for when Link resources change
	linkInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueueLink,
		UpdateFunc: func(old, new interface{}) {
			controller.enqueueLink(new)
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
	if ok := cache.WaitForCacheSync(stopCh, c.linksSynced); !ok {
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
		// Link resource to be synced.
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
	// Convert the namespace/name string into a distinct namespace and name
	log.Printf("And again %v\n", key)
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		runtime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}

	// Get the Link resource with this namespace/name
	link, err := c.linksLister.Links(namespace).Get(name)
	if err != nil {
		// The Link resource may no longer exist, in which case we stop
		// processing.
		if errors.IsNotFound(err) {
			runtime.HandleError(fmt.Errorf("foo '%s' in work queue no longer exists", key))
			// TODO: stop gRPC clients
			return nil
		}

		return err
	}

	if reg, ok := c.carriers[key]; !ok {
		consumer, err := c.consumerFactory(link.Spec.Input, key)
		if err != nil {
			log.Printf("Error creating consumer %v", err)
		} else {

			service, err := c.kubeclientset.CoreV1().Services(namespace).Get(name, v1.GetOptions{})
			if errors.IsNotFound(err) {
				//service = &corev1.Service{Spec:corev1.ServiceSpec{Selector:link.Labels, Ports:[]corev1.ServicePort{Name:}}}
				//service, err = c.kubeclientset.CoreV1().Services(namespace).Create(service)
			}
			if err != nil {
					log.Printf("Error looking up service %v", err)

			} else {
				port := -1
				for _, p := range service.Spec.Ports {
					if p.Name == "grpc" {
						port = int(p.Port)
						break
					}
				}
				hostname := fmt.Sprintf("%s.%s", name, namespace)
				dispatcher, err := grpc.NewGrpcDispatcher(hostname, port, 1*time.Second) // TODO: use svc. TODO: use link.fn.protocol
				if err != nil {
					log.Printf("Error creating dispatcher %v", err)
				} else {
					carrier.Run(consumer, c.producer, dispatcher, link.Spec.Output) // this spawns 2 goroutines
					c.carriers[key] = &registration{c.producer, consumer}

					c.recorder.Event(link, corev1.EventTypeNormal, SuccessSynced, MessageResourceSynced)
				}
			}
			_ = reg
		}
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

// enqueueLink takes a Link resource and converts it into a namespace/name
// string which is then put onto the work queue. This method should *not* be
// passed resources of any type other than Link.
func (c *ctrl) enqueueLink(obj interface{}) {
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		runtime.HandleError(err)
		return
	}
	c.workqueue.AddRateLimited(key)
}
