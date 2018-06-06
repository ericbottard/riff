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
	"github.com/projectriff/riff/kubernetes-crds/pkg/apis/projectriff.io/v1alpha1"
	"github.com/projectriff/riff/function-sidecar/pkg/dispatcher/grpc"
	"time"
	"log"
	"github.com/projectriff/riff/message-transport/pkg/message"
	"fmt"
)

func main() {
	port := 1234
	s := v1alpha1.Windowing{Size: 2}

	d, err := grpc.NewGrpcDispatcher("localhost", port, s, 1000*time.Millisecond)
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			m := <- d.Output()
			log.Printf("Received %v\n", m)
		}
	}()

	i := 0
	for {
		i++
		m := message.NewMessage([]byte(fmt.Sprintf("%v" , i)), nil)
		d.Input() <- m
		time.Sleep(5 * time.Second)
	}

}
