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

package io.sk8s.topic.gateway;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.async.DeferredResult;

/**
 * @author Mark Fisher
 */
@RestController
public class TopicGatewayController {

	@Autowired
	private MessagePublisher publisher;


	@PostMapping(value = "/messages/{topic}")
	public void publishMessage(@PathVariable String topic,
			@RequestBody String payload,
			HttpServletResponse response) throws UnsupportedEncodingException {
		Message<byte[]> msg = MessageBuilder.withPayload(payload.getBytes(StandardCharsets.UTF_8.name()))
				.build();
		this.publisher.publishMessage(topic, msg);
		response.setHeader("Message-Id", msg.getHeaders().getId().toString());
	}

	@PostMapping(value = "/messages/{topic}", params = "async=false")
	public DeferredResult<Object> publishMessageAndReply(@PathVariable String topic,
			@RequestBody String payload) throws UnsupportedEncodingException {
		Message<byte[]> msg = MessageBuilder.withPayload(payload.getBytes(StandardCharsets.UTF_8.name()))
				.build();
		DeferredResult<Object> deferredResult = new DeferredResult<>(10_000L);
		this.publisher.publishMessageExpectReply(topic, msg, m -> deferredResult.setResult(m.getPayload()));
		return deferredResult;
	}
}
