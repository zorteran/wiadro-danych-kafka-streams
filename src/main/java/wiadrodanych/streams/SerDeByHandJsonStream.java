package wiadrodanych.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Produced;
import wiadrodanych.streams.models.Person;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class SerDeByHandJsonStream {

    public static final String INPUT_TOPIC = "wiaderko-input";
    public static final String OUTPUT_TOPIC = "wiaderko-output";

    public static void main(String[] args) throws Exception {
        Properties props = createProperties();

        ObjectMapper mapper = new ObjectMapper();
        final StreamsBuilder builder = new StreamsBuilder();
        builder.<String, String>stream(INPUT_TOPIC)
                .peek(
                    (key, value) -> System.out.println("Input: key=" + key + ", value=" + value)
                )
                .mapValues(v -> {
                    try {
                        return mapper.readValue(v, Person.class);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                })
                .mapValues((v -> {
                    v.name = v.name.substring(0, 1).toUpperCase() + v.name.substring(1).toLowerCase();
                    return v;
                }))
                .filter((k, v) -> v.age >= 18)
                .mapValues(v -> {
                    try {
                        return mapper.writeValueAsString((v));
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                })
                .peek(
                        (key, value) -> System.out.println("Output: key=" + key + ", value=" + value)
                )
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        streams.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
            System.err.println("oh no! error! " + throwable.getMessage());
        });
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    private static Properties createProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wiaderko-json-stream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        return props;
    }
}
