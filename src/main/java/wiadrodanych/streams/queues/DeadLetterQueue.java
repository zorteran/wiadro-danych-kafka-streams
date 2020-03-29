package wiadrodanych.streams.queues;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wiadrodanych.streams.models.Person;

import java.util.Properties;


public class DeadLetterQueue {
    private static final Logger log = LoggerFactory.getLogger(DeadLetterQueue.class);

    private final KafkaProducer<String, String> dlqProducer;
    private final String dlqTopic = "dead_letter_queue";

    private static DeadLetterQueue deadLetterQueueInstance;
    private final Gson gson;

    public static DeadLetterQueue getInstance() {
        if (deadLetterQueueInstance == null) {
            deadLetterQueueInstance = new DeadLetterQueue();
        }
        return deadLetterQueueInstance;
    }

    private DeadLetterQueue() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:29092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        this.dlqProducer = new KafkaProducer<>(props);
        this.gson = new Gson();
    }

    public void send(String key, Person person, Headers headers, String reason) {
        send(key, gson.toJson(person), headers, reason);
    }

    public void send(String key, String value, Headers headers, String reason) throws KafkaException {
        headers.add(new RecordHeader("failure.reason", reason.getBytes()));
        headers.add(new RecordHeader("failure.time", String.valueOf(System.currentTimeMillis()).getBytes()));

        log.warn("Sending to Dead Letter Queue {}: {}", dlqTopic, reason);

        dlqProducer.send(new ProducerRecord<>(
                dlqTopic,
                null,
                key,
                value,
                headers)
        );
    }

    public void send(byte[] key, byte[] value, Headers headers, String reason) throws KafkaException {
        send(new String(key), new String(value), headers, reason);
    }
}