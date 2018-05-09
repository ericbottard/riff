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

package grpc

import (
	"google.golang.org/grpc"

	"fmt"
	"log"
	"time"

	"io"

	"github.com/projectriff/riff/function-sidecar/pkg/dispatcher"
	"github.com/projectriff/riff/function-sidecar/pkg/dispatcher/grpc/function"
	"github.com/projectriff/riff/message-transport/pkg/message"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"encoding/json"
	"os"
	"github.com/projectriff/riff/kubernetes-crds/pkg/apis/projectriff.io/v1alpha1"
)

type grpcDispatcher struct {
	stream           function.MessageFunction_CallClient
	client           function.MessageFunctionClient
	input            chan message.Message
	output           chan message.Message
	closed           chan struct{}
	correlation      CorrelationStrategy
	windowingFactory WindowingStrategyFactory
	streams          map[interface{}]window
}

func (this *grpcDispatcher) Input() chan<- message.Message {
	return this.input
}

func (this *grpcDispatcher) Output() <-chan message.Message {
	return this.output
}

func (this *grpcDispatcher) Closed() <-chan struct{} {
	return this.closed
}

// A CorrelationStrategy groups together messages that belong to the same "spatial" stream (the "temporal" aspect
// of grouping is taken care of by a WindowStrategy). Several invocations for different spatial groups may happen concurrently.
type CorrelationStrategy func(message.Message) interface{}

// A WindowingStrategyFactory is a function for creating a new instance of a (most certainly stateful) WindowingStrategy,
// given a new, first message for a stream.
type WindowingStrategyFactory func(message.Message) WindowingStrategy

//
type WindowingStrategy interface {
	// ShouldClose notifies this strategy that a new message has just been sent to the function, resulting in the optional
	// error passed as 2nd argument.
	// ShouldClose should return true as its first result if this strategy decides that the current stream should end.
	ShouldClose(in message.Message, err error) bool
}

const correlationId = "correlationId"
var headersToForward []string
func init() {
	headersToForward = []string {correlationId}
}


type window struct {
	stream           function.MessageFunction_CallClient
	release          WindowingStrategy
	forwardedHeaders message.Headers
}

func (d *grpcDispatcher) handleIncoming() {
	for in := range d.input {
		// TODO: problem: requires one incoming message to trigger a Call(), what about Supplier functions?
		key := d.correlation(in)
		w, ok := d.streams[key]
		if !ok {
			log.Printf("Opening new Call()\n")
			stream, err := d.client.Call(context.Background())
			if err == nil {
				w.stream = stream
				w.release = d.windowingFactory(in)
				w.forwardedHeaders = retainHeaders(in)
				go w.handleOutgoing(d.output)
			} else {
				// TODO
				log.Printf("Error in open %v\n", err)
			}
			d.streams[key] = w
		} else {
			log.Printf("Re-using existing Call()\n")
		}

		grpcMessage := toGRPC(in)
		err := w.stream.Send(grpcMessage)

		if w.release.ShouldClose(in, err) {
			log.Printf("End of stream, closing\n")
			if err := w.stream.CloseSend(); err != nil {
				// TODO
				log.Printf("Error in close %v\n", err)
			}
			delete(d.streams, key)
		} else {
			log.Printf("Keeping stream open\n")
		}
	}
	fmt.Printf("Done\n")
}

func retainHeaders(in message.Message) message.Headers {
	result := make(message.Headers)
	for _, h := range headersToForward {
		if v, ok := in.Headers()[h] ; ok {
			result[h] = v
		}
	}
	return result
}

func (this *window) handleOutgoing(output chan<- message.Message) {
	for {
		reply, err := this.stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Printf("Error receiving message from function: %v", err)
			break
		}
		message := toDispatcher(reply)
		for h, v := range this.forwardedHeaders {
			if _, ok := message.Headers()[h] ; !ok {
				message.Headers()[h] = v
			}
		}
		output <- message
	}
}

func streamClosureDiagnosed(err error) bool {
	if err == io.EOF {
		return true
	}

	if sErr, ok := status.FromError(err); ok && sErr.Code() == codes.Unavailable {
		// See https://github.com/grpc/grpc/blob/master/doc/statuscodes.md
		log.Printf("Stream to function is closing: %v", err)
		return true
	}

	return false
}

func NewGrpcDispatcher(port int, timeout time.Duration) (dispatcher.Dispatcher, error) {
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	conn, err := grpc.DialContext(ctx, fmt.Sprintf("localhost:%v", port), grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, err
	}

	result := &grpcDispatcher{
		client:         function.NewMessageFunctionClient(conn),
		input:          make(chan message.Message, 100),
		output:         make(chan message.Message, 100),
		closed:         make(chan struct{}),
		streams:        make(map[interface{}]window),
		windowingFactory: unmarshallFactory(),
		correlation: func(m message.Message) interface{} {
			// Correlate by correlationId header by default if present
			if c, ok := m.Headers()[correlationId] ; ok {
				return c[0]
			} else {
				return nil
			}
		},
	}
	go result.handleIncoming()

	return result, nil
}

func toGRPC(message message.Message) *function.Message {
	grpcHeaders := make(map[string]*function.Message_HeaderValue, len(message.Headers()))
	for k, vv := range message.Headers() {
		values := function.Message_HeaderValue{}
		grpcHeaders[k] = &values
		for _, v := range vv {
			values.Values = append(values.Values, v)
		}
	}
	result := function.Message{Payload: message.Payload(), Headers: grpcHeaders}

	return &result
}

func toDispatcher(grpc *function.Message) message.Message {
	dHeaders := make(map[string][]string, len(grpc.Headers))
	for k, pv := range grpc.Headers {
		dHeaders[k] = pv.Values
	}
	return message.NewMessage(grpc.Payload, dHeaders)
}

func unmarshallFactory() WindowingStrategyFactory {
	var w v1alpha1.Windowing
	json.Unmarshal([]byte(os.Getenv("WINDOWING_STRATEGY")), &w)
	// TODO: this can be done reflectively, once there are more alternatives.

	if w.Size != int32(0) && w.Time != "" {
		panic("windowing by time and size are mutually exclusive")
	}

	if w.Size != int32(0) {
		log.Printf("Will use windowing strategy of %v messages\n", w.Size)
		return sizeFactoryFactory(int(w.Size))
	} else if w.Time != "" {
		log.Printf("Will use time based windowing strategy of %v\n", w.Time)
		d, err := time.ParseDuration(w.Time)
		if err != nil {
			panic(err) // Assume it's validated at fn registration time
		}
		return ptimeFactoryFactory(d)
	} else {
		log.Printf("Will use no windowing strategy (unbounded stream)\n")
		return noneFactoryFactory()
	}
}