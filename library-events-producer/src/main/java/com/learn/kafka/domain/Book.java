package com.learn.kafka.domain;

import javax.validation.constraints.NotNull;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class Book {

	@NotNull
	private Integer bookId;
	@NotNull
	private String bookName;
	@NotNull
	private String bookAuthor;
}
