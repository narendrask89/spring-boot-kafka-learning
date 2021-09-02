package com.learn.kafka.repository;

import org.springframework.data.repository.CrudRepository;

import com.learn.kafka.entity.LibraryEvent;

public interface LibraryEventsRepository extends CrudRepository<LibraryEvent, Integer>{

}
