package wiadrodanych.streams.models;

public class Person extends EntityBase {
    public String name;
    public int age;

    public Person(String name, int age) {
        this.name = name;
        this.age = age;
    }
}
