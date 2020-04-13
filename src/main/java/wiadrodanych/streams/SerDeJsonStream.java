package wiadrodanych.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import wiadrodanych.streams.models.Person;
import wiadrodanych.streams.models.serdes.PersonDeserializer;
import wiadrodanych.streams.models.serdes.PersonSerializer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class SerDeJsonStream {

    public static final String INPUT_TOPIC = "wiaderko-input";
    public static final String OUTPUT_TOPIC = "wiaderko-output";

    public static void main(String[] args) throws Exception {
        Properties props = createProperties();

        SerDeJsonStream serDeJsonStream = new SerDeJsonStream();
        final KafkaStreams streams = new KafkaStreams(serDeJsonStream.createTopology(), props);
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

    public Topology createTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        final Serde<Person> personSerde = Serdes.serdeFrom(new PersonSerializer(), new PersonDeserializer());

        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), personSerde))
                .peek(
                        (key, value) -> System.out.println("key=" + key + ", value=" + value)
                )
                .mapValues((v -> {
                    v.name = v.name.substring(0, 1).toUpperCase() + v.name.substring(1).toLowerCase();
                    return v;
                }))
                .filter((k, v) -> v.age >= 18)
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), personSerde));

        return builder.build();
    }

    private static Properties createProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wiaderko-json-stream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        return props;
    }
}
