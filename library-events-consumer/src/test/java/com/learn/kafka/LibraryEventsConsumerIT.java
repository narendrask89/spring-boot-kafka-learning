package com.learn.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.kafka.consumer.LibraryEventsConsumer;
import com.learn.kafka.entity.Book;
import com.learn.kafka.entity.LibraryEvent;
import com.learn.kafka.entity.LibraryEventType;
import com.learn.kafka.repository.LibraryEventsRepository;
import com.learn.kafka.service.LibraryEventsService;

import lombok.SneakyThrows;

@SpringBootTest
@EmbeddedKafka(topics = { "library-event" }, partitions = 3)
@TestPropertySource(properties = { "spring.kafka.producer.bootstrap-servers = ${spring.embedded.kafka.brokers}",
		"spring.kafka.consumer.bootstrap-servers = ${spring.embedded.kafka.brokers}" })
public class LibraryEventsConsumerIT {

	@Autowired
	EmbeddedKafkaBroker embeddedKafkaBroker;

	@Autowired
	KafkaTemplate<Integer, String> kafkaTemplate;

	@Autowired
	KafkaListenerEndpointRegistry endpointRegistry;

	@SpyBean
	LibraryEventsConsumer libraryEventsConsumerSpy;

	@SpyBean
	LibraryEventsService libraryEventsServiceSpy;

	@Autowired
	LibraryEventsRepository libraryEventsRepository;

	@Autowired
	ObjectMapper objectMapper;

	@BeforeEach
	void setup() {
		for (MessageListenerContainer messageListenerContainer : endpointRegistry.getAllListenerContainers()) {
			ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
		}
	}

	@Test
	@SneakyThrows
	void publishNewLibraryEvents() {
		// given
		String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":123,\"bookName\":\"Learn Spring Boot with Kafka\",\"bookAuthor\":\"Narendra Sahu\"}}";
		kafkaTemplate.sendDefault(json).get();

		// when
		CountDownLatch countDownLatch = new CountDownLatch(1);
		countDownLatch.await(3, TimeUnit.SECONDS);

		// then
		verify(libraryEventsConsumerSpy, atLeast(1)).doOnMessage(isA(ConsumerRecord.class));
		verify(libraryEventsServiceSpy, atLeast(1)).processLibraryEvents(isA(ConsumerRecord.class));

		List<LibraryEvent> libraryEvents = (List<LibraryEvent>) libraryEventsRepository.findAll();
		assert libraryEvents.size() == 1;

		libraryEvents.forEach(libraryEvent -> {
			assert libraryEvent.getLibraryEventId() != null;
			assertEquals(123, libraryEvent.getBook().getBookId());
		});
	}

	@Test
	@SneakyThrows
	void publishUpdateLibraryEvents() {
		// given
		String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":123,\"bookName\":\"Learn Spring Boot with Kafka\",\"bookAuthor\":\"Narendra Sahu\"}}";
		LibraryEvent libraryEvent = objectMapper.readValue(json, LibraryEvent.class);
		libraryEvent.getBook().setLibraryEvent(libraryEvent);
		libraryEventsRepository.save(libraryEvent);

		Book updatedBook = Book.builder().bookId(123).bookAuthor("Narendra Sahu").bookName("Spring Book 2.x with Kafka")
				.build();
		libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
		libraryEvent.setBook(updatedBook);
		String updatedJson = objectMapper.writeValueAsString(libraryEvent);
		kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(), updatedJson).get();

		// when
		CountDownLatch countDownLatch = new CountDownLatch(1);
		countDownLatch.await(3, TimeUnit.SECONDS);

		// then
		LibraryEvent persistedLibraryEvent = libraryEventsRepository.findById(libraryEvent.getLibraryEventId()).get();
		assertEquals("Spring Book 2.x with Kafka", persistedLibraryEvent.getBook().getBookName());

	}

	@Test()
	@SneakyThrows
	void publishModifyLibraryEvents() {
		// given
		String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":123,\"bookName\":\"Learn Spring Boot with Kafka\",\"bookAuthor\":\"Narendra Sahu\"}}";
		LibraryEvent libraryEvent = objectMapper.readValue(json, LibraryEvent.class);
		libraryEvent.getBook().setLibraryEvent(libraryEvent);
		libraryEventsRepository.save(libraryEvent);

		Book updatedBook = Book.builder().bookId(123).bookAuthor("Narendra Sahu").bookName("Spring Book 2.x with Kafka")
				.build();
		libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
		libraryEvent.setBook(updatedBook);
		libraryEvent.setLibraryEventId(123);
		String updatedJson = objectMapper.writeValueAsString(libraryEvent);
		kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(), updatedJson).get();

		// when
		CountDownLatch countDownLatch = new CountDownLatch(1);
		countDownLatch.await(3, TimeUnit.SECONDS);

	}

	@Test
	@SneakyThrows
	void publishModifyLibraryEvent_Not_A_Valid_LibraryEventId() {
		// given
		String json = "{\"libraryEventId\":123,\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":123,\"bookName\":\"Learn Spring Boot with Kafka\",\"bookAuthor\":\"Narendra Sahu\"}}";
		kafkaTemplate.sendDefault(123, json).get();
		// when
		CountDownLatch latch = new CountDownLatch(1);
		latch.await(3, TimeUnit.SECONDS);

		verify(libraryEventsConsumerSpy, atLeast(1)).doOnMessage(isA(ConsumerRecord.class));
		verify(libraryEventsServiceSpy, atLeast(1)).processLibraryEvents(isA(ConsumerRecord.class));

		Optional<LibraryEvent> libraryEventOptional = libraryEventsRepository.findById(123);
		assertFalse(libraryEventOptional.isPresent());
	}

	@Test
	@SneakyThrows
	void publishModifyLibraryEvent_Null_LibraryEventId() {
		// given
		String json = "{\"libraryEventId\":null,\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":123,\"bookName\":\"Learn Spring Boot with Kafka\",\"bookAuthor\":\"Narendra Sahu\"}}";
		kafkaTemplate.sendDefault(null, json).get();
		// when
		CountDownLatch latch = new CountDownLatch(1);
		latch.await(3, TimeUnit.SECONDS);

		verify(libraryEventsConsumerSpy, atLeast(1)).doOnMessage(isA(ConsumerRecord.class));
		verify(libraryEventsServiceSpy, atLeast(1)).processLibraryEvents(isA(ConsumerRecord.class));
	}

	@AfterEach
	void tearDown() {
		libraryEventsRepository.deleteAll();
	}
}
