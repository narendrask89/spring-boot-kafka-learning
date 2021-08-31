package com.learn.kafka.unit;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.kafka.controller.LibraryEventsController;
import com.learn.kafka.domain.Book;
import com.learn.kafka.domain.LibraryEvent;
import com.learn.kafka.producer.LibraryEventProducer;

@WebMvcTest(LibraryEventsController.class)
@AutoConfigureMockMvc
public class LibraryEventsControllerUnitTest {

	@Autowired
	MockMvc mockMvc;
	
	@MockBean
	LibraryEventProducer libraryEventProducer;

	ObjectMapper mapper = new ObjectMapper();

	@Test
	void postLibraryEvent() throws Exception {
		// given
		Book book = Book.builder().bookName("Test").bookId(123).bookAuthor("Narendra").build();

		LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).book(book).build();

		String json = mapper.writeValueAsString(libraryEvent);
		when(libraryEventProducer.sendLibraryEvent_Approach2(isA(LibraryEvent.class))).thenReturn(null);
		
		// when
		mockMvc.perform(post("/v1/libraryevent").content(json).contentType(MediaType.APPLICATION_JSON))
				.andExpect(status().isCreated());
	}
	
	@Test
	void postLibraryEvent_4xx() throws Exception {
		// given
		Book book = Book.builder().bookName(null).bookId(null).bookAuthor("Narendra").build();
		LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).book(book).build();

		String json = mapper.writeValueAsString(libraryEvent);
		when(libraryEventProducer.sendLibraryEvent_Approach2(isA(LibraryEvent.class))).thenReturn(null);
		
		// when
		String expectedErrorMessage= "book.bookId - must not be null, book.bookName - must not be null";
		mockMvc.perform(post("/v1/libraryevent").content(json).contentType(MediaType.APPLICATION_JSON))
				.andExpect(status().is4xxClientError()).andExpect(content().string(expectedErrorMessage));
	}
	
	@Test
	void updateLibraryEvent() throws Exception {
		// given
		Book book = Book.builder().bookName("Learn Spring Boot with kafka").bookId(123).bookAuthor("Narendra Sahu").build();

		LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(123).book(book).build();

		String json = mapper.writeValueAsString(libraryEvent);
		when(libraryEventProducer.sendLibraryEvent_Approach2(isA(LibraryEvent.class))).thenReturn(null);
		
		// when
		mockMvc.perform(put("/v1/libraryevent").content(json).contentType(MediaType.APPLICATION_JSON))
				.andExpect(status().isOk());
	}
	
	@Test
	void updateLibraryEvent_withNullLibraryEventId() throws Exception {
		// given
		Book book = Book.builder().bookName("Learn Spring Boot with kafka").bookId(null).bookAuthor("Narendra Sahu").build();
		LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).book(book).build();

		String json = mapper.writeValueAsString(libraryEvent);
		when(libraryEventProducer.sendLibraryEvent_Approach2(isA(LibraryEvent.class))).thenReturn(null);
		
		// when
		mockMvc.perform(put("/v1/libraryevent").content(json).contentType(MediaType.APPLICATION_JSON))
				.andExpect(status().isBadRequest()).andExpect(content().string("Please pass the libraryEventId in the request"));
	}
}
