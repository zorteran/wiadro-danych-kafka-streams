package wiadrodanych.streams.models;

public class EntityBase {
    public transient boolean valid;
    public transient Exception exception;

    public EntityBase() {
        valid = true;
    }
}
