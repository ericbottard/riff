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

import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import org.springframework.cloud.stream.binder.EmbeddedHeaderUtils;
import org.springframework.cloud.stream.binder.MessageValues;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.context.request.async.DeferredResult;

/**
 * @author Mark Fisher
 */
public class MessagePublisher {

	private String replyChannel;

	private final KafkaTemplate<String, byte[]> kafkaTemplate;

	private final Consumer<String, byte[]> consumer;

	private Map<UUID, java.util.function.Consumer<Message>> results =new ConcurrentHashMap<>();


	public MessagePublisher(KafkaTemplate<String, byte[]> kafkaTemplate, Consumer<String, byte[]> consumer) {
		this.kafkaTemplate = kafkaTemplate;
		this.consumer = consumer;
		replyChannel = "replies_" + System.currentTimeMillis();
		replyChannel = "replies";
		this.consumer.assign(Arrays.asList(new TopicPartition(replyChannel, 1)));
		new Thread(new ResultRunnable()).start();
	}

	public void publishMessage(String topic, Message message) {
		try {
			byte[] bytes = EmbeddedHeaderUtils.embedHeaders(new MessageValues(message), MessageHeaders.ID, MessageHeaders.REPLY_CHANNEL);
			this.kafkaTemplate.send(topic, bytes);
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void publishMessageExpectReply(String topic, Message message, java.util.function.Consumer<Message> resultSetter) {
		Message enhanced = MessageBuilder.fromMessage(message).setReplyChannelName(replyChannel).build();
		results.put(enhanced.getHeaders().getId(), resultSetter);
		publishMessage(topic, enhanced);
	}

	private class ResultRunnable implements Runnable {

		@Override
		public void run() {
			while (true) {
				ConsumerRecords<String, byte[]> records = consumer.poll(10_000);
				for (ConsumerRecord<String, byte[]> record : records.records(replyChannel)) {
					try {
						MessageValues msg = EmbeddedHeaderUtils.extractHeaders(record.value());
						Object originalId = msg.getHeaders().get(IntegrationMessageHeaderAccessor.CORRELATION_ID);
						results.remove(originalId).accept(msg.toMessage());
					}
					catch (Exception e) {
						e.printStackTrace();
					}

				}
			}
		}
	}
}
