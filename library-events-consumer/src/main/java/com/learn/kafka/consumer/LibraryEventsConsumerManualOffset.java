package com.learn.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

import lombok.extern.slf4j.Slf4j;

//@Component
@Slf4j
public class LibraryEventsConsumerManualOffset implements AcknowledgingMessageListener<Integer, String> {

	
	@KafkaListener(topics = {"library-event"})
	public void onMessage(ConsumerRecord<Integer, String> consumerRecord, Acknowledgment acknowledgment) {
		log.info("ConsumerRecords : {}", consumerRecord);
		acknowledgment.acknowledge();
	}
}
