package com.learn.kafka.producer;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.kafka.domain.LibraryEvent;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class LibraryEventProducer {

	private static final String LIBRARY_EVENT_TOPIC = "library-event";

	@Autowired
	KafkaTemplate<Integer, String> kafkaTemplate;

	@Autowired
	ObjectMapper objectMapper;

	public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {
		Integer key = libraryEvent.getLibraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);
		ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.sendDefault(key, value);
		listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

			@Override
			public void onFailure(Throwable ex) {
				handleFailure(key, value, ex);
			}

			@Override
			public void onSuccess(SendResult<Integer, String> result) {
				handleSuccess(key, value, result);
			}

		});
	}

	public ListenableFuture<SendResult<Integer, String>> sendLibraryEvent_Approach2(LibraryEvent libraryEvent) throws Exception {
		Integer key = libraryEvent.getLibraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);
		ProducerRecord<Integer, String> producerRecord = buildProducerRecord(key, value, LIBRARY_EVENT_TOPIC);
		ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.send(producerRecord);
		listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

			@Override
			public void onFailure(Throwable ex) {
				handleFailure(key, value, ex);
			}

			@Override
			public void onSuccess(SendResult<Integer, String> result) {
				handleSuccess(key, value, result);
			}

		});
		
		return listenableFuture;
	}

	private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value, String libraryEventTopic) {

		List<Header> recordHeaders = List.of(new RecordHeader("event-source", "source".getBytes()));
		return new ProducerRecord<Integer, String>(libraryEventTopic, null, key, value, recordHeaders);
	}

	public SendResult<Integer, String> sendLibraryEventSynchronous(LibraryEvent libraryEvent) throws Exception {
		Integer key = libraryEvent.getLibraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);
		SendResult<Integer, String> sendResult = null;
		try {
			sendResult = kafkaTemplate.sendDefault(key, value).get(1, TimeUnit.SECONDS);
		} catch (InterruptedException | ExecutionException e) {
			log.error("ExecutionException / InterruptedException Error in onFailure: {}", e.getMessage());
			throw e;
		}

		return sendResult;

	}

	private void handleFailure(Integer key, String value, Throwable ex) {
		log.error("Error sending the message and the exception is {}", ex.getMessage());
		try {
			throw ex;
		} catch (Throwable throwable) {
			log.error("Error in onFailure: {}", throwable.getMessage());
		}
	}

	private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
		log.info("Message sent successfully for the key : {} and the value is {} , partition is {}", key, value,
				result.getRecordMetadata().partition());
	}
}
