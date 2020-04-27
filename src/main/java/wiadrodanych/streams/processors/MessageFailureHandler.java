package wiadrodanych.streams.processors;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wiadrodanych.streams.models.Person;
import wiadrodanych.streams.queues.DeadLetterQueue;

public class MessageFailureHandler implements Processor<String, Person> {
    private ProcessorContext context;

    private static final Logger log = LoggerFactory.getLogger(MessageFailureHandler.class);

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public void process(String key, Person value) {

        DeadLetterQueue.getInstance().send(
                key,
                value,
                context.headers(),
                value.exception.toString()
        );
    }

    @Override
    public void close() {
    }
}
