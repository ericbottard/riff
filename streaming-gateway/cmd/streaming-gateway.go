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

package main

import (
	"flag"
	"os"
	"strings"
	"time"

	"github.com/golang/glog"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	clientset "github.com/projectriff/riff/kubernetes-crds/pkg/client/clientset/versioned"
	informers "github.com/projectriff/riff/kubernetes-crds/pkg/client/informers/externalversions"
	"github.com/projectriff/riff/streaming-gateway/pkg/signals"
	"github.com/projectriff/riff/streaming-gateway/pkg/controller"
	"github.com/projectriff/riff/message-transport/pkg/transport/kafka"
	"github.com/projectriff/riff/message-transport/pkg/transport"
	"github.com/bsm/sarama-cluster"
	"github.com/Shopify/sarama"
)

func main() {

	kubeconfig := flag.String("kubeconf", "", "Path to a kube config. Only required if out-of-cluster.")
	masterURL := flag.String("master-url", "", "Path to master URL. Useful eg when using proxy")
	flag.Parse()

	brokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
	_ = brokers

	// set up signals so we handle the first shutdown signal gracefully
	stopCh := signals.SetupSignalHandler()

	cfg, err := clientcmd.BuildConfigFromFlags(*masterURL, *kubeconfig)
	if err != nil {
		glog.Fatalf("Error building kubeconfig: %s", err.Error())
	}

	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		glog.Fatalf("Error building kubernetes clientset: %s", err.Error())
	}

	exampleClient, err := clientset.NewForConfig(cfg)
	if err != nil {
		glog.Fatalf("Error building riff clientset: %s", err.Error())
	}

	kubeInformerFactory := kubeinformers.NewSharedInformerFactory(kubeClient, time.Second*30)
	riffInformerFactory := informers.NewSharedInformerFactory(exampleClient, time.Second*30)



	consumerConfig := makeConsumerConfig()
	consumerConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumerFactory := func(topic string, group string) (transport.Consumer, error) {
		return kafka.NewConsumer(brokers, group, []string{topic}, consumerConfig)
	}
	producer, err := kafka.NewProducer(brokers)
	if err != nil {
		panic(err)
	}

	controller := controller.NewController(kubeClient,
		exampleClient,
		riffInformerFactory.Projectriff().V1alpha1().Links(),
		consumerFactory,
		producer,
	)

	go kubeInformerFactory.Start(stopCh)
	go riffInformerFactory.Start(stopCh)

	if err = controller.Run(2, stopCh); err != nil {
		glog.Fatalf("Error running controller: %s", err.Error())
	}

}

func makeConsumerConfig() *cluster.Config {
	consumerConfig := cluster.NewConfig()
	consumerConfig.Consumer.Return.Errors = true
	consumerConfig.Group.Return.Notifications = true
	return consumerConfig
}
