package wiadrodanych.streams.processors;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;

public class ZtmProcessorSupplier implements ProcessorSupplier {
    @Override
    public Processor get() {
        return new ZtmProcessor();
    }
}