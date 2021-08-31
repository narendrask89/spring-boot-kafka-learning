package com.learn.kafka.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import com.learn.kafka.domain.Book;
import com.learn.kafka.domain.LibraryEvent;

@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = { "library-event" }, partitions = 3)
@TestPropertySource(properties = { "spring.kafka.producer.bootstrap-servers = ${spring.embedded.kafka.brokers}",
		"spring.kafka.admin.properties.bootstrap.servers = ${spring.embedded.kafka.brokers}" })
public class LibraryEventsControllerIT {

	@Autowired
	TestRestTemplate restTemplate;

	@Autowired
	EmbeddedKafkaBroker embeddedKafkaBroker;

	private Consumer<Integer, String> consumer;

	@BeforeEach
	void setUp() {
		Map<String, Object> configs = new HashMap<>(
				KafkaTestUtils.consumerProps("grooup1", "true", embeddedKafkaBroker));
		consumer = new DefaultKafkaConsumerFactory<Integer, String>(configs, new IntegerDeserializer(), new StringDeserializer())
				.createConsumer();
		embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
	}

	@Test
	void postLibraryEvents() {
		// given
		Book book = Book.builder().bookName("Test").bookId(123).bookAuthor("Narendra").build();

		LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).book(book).build();

		HttpHeaders headers = new HttpHeaders();
		headers.set("content-type", MediaType.APPLICATION_JSON_VALUE.toString());

		HttpEntity<LibraryEvent> httpEntity = new HttpEntity<LibraryEvent>(libraryEvent, headers);

		// when
		ResponseEntity<LibraryEvent> responseEntity = restTemplate.exchange("/v1/libraryevent", HttpMethod.POST,
				httpEntity, LibraryEvent.class);
		// then

		assertEquals(HttpStatus.CREATED, responseEntity.getStatusCode());

		ConsumerRecord<Integer, String> record = KafkaTestUtils.getSingleRecord(consumer, "library-event");
		String expected = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":123,\"bookName\":\"Test\",\"bookAuthor\":\"Narendra\"}}";
		String value = record.value();
		assertEquals(expected, value);
	}
}
