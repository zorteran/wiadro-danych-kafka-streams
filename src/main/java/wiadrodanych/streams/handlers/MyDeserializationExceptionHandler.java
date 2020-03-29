package wiadrodanych.streams.handlers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wiadrodanych.streams.queues.DeadLetterQueue;

import java.util.Map;

public class MyDeserializationExceptionHandler implements DeserializationExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(MyDeserializationExceptionHandler.class);

    @Override
    public DeserializationHandlerResponse handle(ProcessorContext context, ConsumerRecord<byte[], byte[]> record, Exception exception) {
        exception.printStackTrace();
        log.warn("Exception caught during Deserialization, taskId: {}, topic: {}, partition: {}, offset: {}", new Object[]{context.taskId(), record.topic(), record.partition(), record.offset(), exception});
        DeadLetterQueue dlq = DeadLetterQueue.getInstance();
        dlq.send(record.key(), record.value(), record.headers(), exception.getMessage());
        return DeserializationHandlerResponse.CONTINUE;
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
