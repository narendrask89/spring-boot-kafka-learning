package com.learn.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.learn.kafka.service.LibraryEventsService;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class LibraryEventsConsumer {

	@Autowired
	private LibraryEventsService libraryEventsService;
	
	@KafkaListener(topics = {"library-event"})
	public void doOnMessage(ConsumerRecord<Integer, String> consumerRecord) throws JsonMappingException, JsonProcessingException {
		log.info("ConsumerRecords : {}", consumerRecord);
		libraryEventsService.processLibraryEvents(consumerRecord);
	}
}
