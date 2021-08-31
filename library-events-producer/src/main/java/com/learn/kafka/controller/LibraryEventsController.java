package com.learn.kafka.controller;

import javax.validation.Valid;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learn.kafka.domain.LibraryEvent;
import com.learn.kafka.domain.LibraryEventType;
import com.learn.kafka.producer.LibraryEventProducer;

import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
public class LibraryEventsController {
	
	@Autowired
	LibraryEventProducer libraryEventProducer;

	@PostMapping("/v1/libraryevent")
	public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws Exception {

		log.info("before sendLibraryEvent");
		libraryEvent.setLibraryEventType(LibraryEventType.NEW);
		//libraryEventProducer.sendLibraryEvent(libraryEvent);
		//SendResult<Integer, String> sendResult = libraryEventProducer.sendLibraryEventSynchronous(libraryEvent);
		//log.info("send result is {}", sendResult.toString());
		libraryEventProducer.sendLibraryEvent_Approach2(libraryEvent);
		log.info("after sendLibraryEvent");
		return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
	}
	
	@PutMapping("/v1/libraryevent")
	public ResponseEntity<LibraryEvent> putLibraryEvent(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {

		log.info("before sendLibraryEvent");
		libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
		libraryEventProducer.sendLibraryEvent(libraryEvent);
		log.info("after sendLibraryEvent");
		return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
	}
}
