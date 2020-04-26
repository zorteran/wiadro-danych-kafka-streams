package wiadrodanych.streams.processors;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;

public class MessageFailureHandlerSupplier implements ProcessorSupplier {
    @Override
    public Processor get() {
        return new MessageFailureHandler();
    }
}