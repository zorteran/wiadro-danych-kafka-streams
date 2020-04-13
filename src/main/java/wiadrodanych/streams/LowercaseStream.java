package wiadrodanych.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class LowercaseStream {

    public static final String INPUT_TOPIC = "wiaderko-input";
    public static final String OUTPUT_TOPIC = "wiaderko-output";

    public static void main(String[] args) throws Exception {
        Properties props = createProperties();

        LowercaseStream lowercaseStream = new LowercaseStream();

        final KafkaStreams streams = new KafkaStreams(lowercaseStream.createTopology(), props);
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
        builder.<String, String>stream(INPUT_TOPIC)
                .peek(
                        (key, value) -> System.out.println("Input: key=" + key + ", value=" + value)
                )
                .mapValues(v -> v.toLowerCase())
                .peek(
                        (key, value) -> System.out.println("Output: key=" + key + ", value=" + value)
                )
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

    private static Properties createProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wiaderko-lowercase-stream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }
}
