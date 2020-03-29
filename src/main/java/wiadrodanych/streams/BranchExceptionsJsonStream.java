package wiadrodanych.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wiadrodanych.streams.handlers.MyDeserializationExceptionHandler;
import wiadrodanych.streams.models.Person;
import wiadrodanych.streams.models.serdes.PersonDeserializer;
import wiadrodanych.streams.models.serdes.PersonSerializer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class BranchExceptionsJsonStream {

    public static final String INPUT_TOPIC = "wiaderko-input";
    public static final String OUTPUT_TOPIC = "wiaderko-output";
    public static final String DLQ_TOPIC = "dead_letter_queue";
    private static final Logger log = LoggerFactory.getLogger(FilterExceptionsJsonStream.class);

    public static void main(String[] args) throws Exception {
        Properties props = createProperties();

        ObjectMapper mapper = new ObjectMapper();
        final StreamsBuilder builder = new StreamsBuilder();
        final Serde<Person> personSerde = Serdes.serdeFrom(new PersonSerializer(), new PersonDeserializer());

int valid = 0;
int invalid = 1;

KStream<String, Person>[] personStream = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), personSerde))
        .peek(
                (key, value) -> System.out.println("key=" + key + ", value=" + value)
        )
        .mapValues(v -> {
            try {
                v.name = v.name.substring(0, 1).toUpperCase() + v.name.substring(1).toLowerCase();
                return v;
            } catch (Exception exception) {
                System.err.println("Occured an exception has... ");
                exception.printStackTrace();
                v.valid = false;
                return v;
            }
        })
        .branch(
                (key, value) -> value.valid,
                (key, value) -> true // !value.valid
        );

personStream[valid].filter((k, v) -> v.age >= 18)
        .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), personSerde));

personStream[invalid].to(DLQ_TOPIC, Produced.with(Serdes.String(), personSerde));

        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        streams.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
            System.err.println("Occured an uncaught exception has...");
            throwable.printStackTrace();
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
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, MyDeserializationExceptionHandler.class);
        return props;
    }
}
