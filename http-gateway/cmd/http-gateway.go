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
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/bsm/sarama-cluster"
	"github.com/projectriff/riff/http-gateway/pkg/server"
	"github.com/projectriff/riff/message-transport/pkg/transport/kafka"
	"github.com/golang/glog"
	informersV1 "github.com/projectriff/riff/kubernetes-crds/pkg/client/informers/externalversions/projectriff/v1"
	"k8s.io/client-go/tools/clientcmd"
	riffcs "github.com/projectriff/riff/kubernetes-crds/pkg/client/clientset/versioned"
	informers "github.com/projectriff/riff/kubernetes-crds/pkg/client/informers/externalversions"

	"k8s.io/client-go/rest"
	"flag"
)

func main() {
	kubeconf := flag.String("kubeconf", "", "Path to a kube config. Only required if out-of-cluster.")
	flag.Parse()

	brokers := brokers()
	producer, err := kafka.NewProducer(brokers)
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	consumer, err := kafka.NewConsumer(brokers, "gateway", []string{"replies"}, cluster.NewConfig())
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	topicInformer := makeTopicInformer(kubeconf)

	gw := server.New(8080, producer, consumer, 60*time.Second, topicInformer)

	done := make(chan struct{})
	gw.Run(done)

	// Wait for shutdown
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM, os.Kill)
	<-signals
	log.Println("Shutting Down...")
	close(done)

}

func brokers() []string {
	return strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
}

func makeTopicInformer(kubeconf *string) (informersV1.TopicInformer) {
	riffClient := riffClientSet(kubeconf)
	riffInformerFactory := informers.NewSharedInformerFactory(riffClient, time.Second*30)
	topicsInformer := riffInformerFactory.Projectriff().V1().Topics()
	return topicsInformer
}

func riffClientSet(kubeconf *string) *riffcs.Clientset {
	config, err := getClientConfig(*kubeconf)
	if err != nil {
		glog.Fatalf("Error getting client config: %s", err.Error())
	}
	riffClient, err := riffcs.NewForConfig(config)
	if err != nil {
		glog.Fatalf("Error building riff clientset: %s", err.Error())
	}
	return riffClient
}

// return rest config, if path not specified assume in cluster config
func getClientConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return rest.InClusterConfig()
}

