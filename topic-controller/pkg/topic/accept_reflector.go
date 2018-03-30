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

package topic

import (
	"github.com/projectriff/riff/kubernetes-crds/pkg/apis/projectriff.io/v1"
	"github.com/projectriff/riff/kubernetes-crds/pkg/client/clientset/versioned"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"fmt"
)

type topicKey string // Simply topic name for now, may end up including eg namespace

type acceptRelector struct {
	downstreamFunctions map[topicKey][]*v1.Function
	clientset *versioned.Clientset
}

func (r *acceptRelector) add(f *v1.Function) {
	key := topicKey(f.Spec.Input)
	r.downstreamFunctions[key] = append(r.downstreamFunctions[key], f)
	r.update(key)
}

func (r *acceptRelector) remove(fun *v1.Function) {
	key := topicKey(fun.Spec.Input)
	functions := r.downstreamFunctions[key]
	for i, f := range functions {
		if f.Name == fun.Name && f.Namespace == fun.Namespace {
			functions[i] = functions[len(functions)-1]
			functions[len(functions)-1] = nil
			r.downstreamFunctions[key] = functions[:len(functions) - 1]
			break
		}
	}
	r.update(key)
}

func (r *acceptRelector) update(topic topicKey) {
	intersection := v1.AcceptedMediaTypes{v1.MediaType("*/*"):v1.Quality("1.0")}
	for _, f := range r.downstreamFunctions[topic] {
		if f.Status != nil && f.Status.Accept != nil {
			intersection = intersection.Intersect(f.Status.Accept)
		}
	}
	t, err := r.clientset.ProjectriffV1().Topics("default" /*TODO*/).Get(string(topic), metaV1.GetOptions{})
	if err != nil {
		fmt.Printf("Oops %v", err)
		return
	}
	t.Status = &v1.TopicStatus{Accept: intersection}
	t, err = r.clientset.ProjectriffV1().Topics("default" /*TODO*/).Update(t)
	if err != nil {
		fmt.Printf("Oops2 %v", err)
		return
	}
}

func NewAcceptReflector(clientset *versioned.Clientset) *acceptRelector {
	return &acceptRelector{clientset:clientset, downstreamFunctions:make(map[topicKey][]*v1.Function)}
}