package com.learn.kafka.unit;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.kafka.domain.Book;
import com.learn.kafka.domain.LibraryEvent;
import com.learn.kafka.producer.LibraryEventProducer;

@ExtendWith(MockitoExtension.class)
public class LibraryEventProducerUnitTest {

	@Mock
	KafkaTemplate<Integer, String> kafkaTemplate;

	@InjectMocks
	LibraryEventProducer libraryEventProducer;

	@Spy
	ObjectMapper objectMapper = new ObjectMapper();

	@Test
	void sendLibraryEvents_approach2_failure() {
		// given
		Book book = Book.builder().bookName("Test").bookId(123).bookAuthor("Narendra").build();

		LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).book(book).build();

		SettableListenableFuture future = new SettableListenableFuture<>();
		future.setException(new RuntimeException("Excetion calling kafka"));

		when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);
		// when
		assertThrows(Exception.class, () -> libraryEventProducer.sendLibraryEvent_Approach2(libraryEvent).get());
	}

	@Test
	void sendLibraryEvents_approach2_success() throws Exception {
		// given
		Book book = Book.builder().bookName("Test").bookId(123).bookAuthor("Narendra").build();

		LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).book(book).build();
		String value = objectMapper.writeValueAsString(libraryEvent);

		SettableListenableFuture future = new SettableListenableFuture<>();
		RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("library-event", 1), 1, 1, 342,
				System.currentTimeMillis(), 1, 2);
		ProducerRecord<Integer, String> producerRecord = new ProducerRecord<Integer, String>("library-event",
				libraryEvent.getLibraryEventId(), value);
		SendResult<Integer, String> sendResult = new SendResult<>(producerRecord, recordMetadata);
		future.set(sendResult);

		when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);
		// when
		ListenableFuture<SendResult<Integer, String>> future2 = libraryEventProducer.sendLibraryEvent_Approach2(libraryEvent);
		
		//then
		
		SendResult<Integer, String> sendResult2 = future2.get();
		assert sendResult2.getRecordMetadata().partition() == 1;
		
	}
}
