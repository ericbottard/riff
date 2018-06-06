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
	"net"
	"fmt"
	grpc2 "google.golang.org/grpc"
	"github.com/projectriff/riff/function-sidecar/pkg/dispatcher/grpc/function"
	"io"
	"strconv"
)

func main() {
	port := 1234
	var err error

	server := grpc2.NewServer()
	function.RegisterMessageFunctionServer(server, &myfunction{})

	l, err := net.Listen("tcp", fmt.Sprintf(":%v", port))
	if err != nil {
		panic(err)
	}

	server.Serve(l)

}


type myfunction struct {
}

func (*myfunction) Call(stream function.MessageFunction_CallServer) error {
	sum := 0
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		value, err := strconv.Atoi(string(in.Payload))
		if err != nil {
			return err
		}
		sum += value
		out := function.Message{Payload: []byte(strconv.Itoa(sum))}
		err = stream.Send(&out)
		if err != nil {
			return err
		}
	}
}
