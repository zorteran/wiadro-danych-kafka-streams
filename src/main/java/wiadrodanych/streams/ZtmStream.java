package wiadrodanych.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import wiadrodanych.streams.models.ZtmRecord;
import wiadrodanych.streams.models.serdes.GenericSerializer;
import wiadrodanych.streams.models.serdes.InputZtmRecordToZtmRecordDeserializer;
import wiadrodanych.streams.models.serdes.ZtmRecordDeserializer;
import wiadrodanych.streams.processors.ZtmProcessor;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ZtmStream {

    public static final String INPUT_TOPIC = "ztm-input";
    public static final String OUTPUT_TOPIC = "ztm-output";

    public ZtmStream() {
    }

    public static void main(String[] args) throws Exception {
        Properties props = createProperties();

        ZtmStream serDeJsonStream = new ZtmStream();
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
        final Serde<ZtmRecord> outputZtmRecordSerde = Serdes.serdeFrom(new GenericSerializer(), new ZtmRecordDeserializer());///new ZtmRecordDeserializer());

        StoreBuilder ztmStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("ztmStore"),
                        Serdes.String(),
                        outputZtmRecordSerde
                );

        Topology topology = new Topology();
        topology.addSource("Source",new StringDeserializer(), new InputZtmRecordToZtmRecordDeserializer(),INPUT_TOPIC)
                .addProcessor("ZtmProcess", () -> new ZtmProcessor(), "Source")
                .addStateStore(ztmStoreBuilder, "ZtmProcess")
                .addSink("Sink", OUTPUT_TOPIC, new StringSerializer(), new GenericSerializer(),"ZtmProcess");

        return topology;
    }

    private static Properties createProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wiaderko-ztm-stream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        return props;
    }
}
