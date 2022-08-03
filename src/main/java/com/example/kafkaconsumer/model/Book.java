package com.example.kafkaconsumer.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ToString
@Getter
@Setter
public class Book {
    private long bookId;
    private String title;

    public Book(){

    }
}
