package wiadrodanych.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class LowercaseStreamTest {

    TopologyTestDriver testDriver;

    StringSerializer stringSerializer = new StringSerializer();
    StringDeserializer stringDeserializer = new StringDeserializer();
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, String> outputTopic;

    @Before
    public void prepareTopologyTestDriver() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "LowercaseStreamTest");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "doesnt-matter:1337");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        LowercaseStream lowercaseStream = new LowercaseStream();
        Topology topology = lowercaseStream.createTopology();
        testDriver = new TopologyTestDriver(topology, config);
        inputTopic = testDriver.createInputTopic(LowercaseStream.INPUT_TOPIC, stringSerializer, stringSerializer);
        outputTopic = testDriver.createOutputTopic(LowercaseStream.OUTPUT_TOPIC, stringDeserializer, stringDeserializer);
    }

    @After
    public void closeTestDriver() {
        testDriver.close();
    }

    @Test
    public void outputShouldBeLowercase()
    {
        String inputText = "Wiadro Danych Rul3Z!";
        inputTopic.pipeInput(inputText);
        String outputText = outputTopic.readValue();
        Assert.assertEquals(outputText, inputText.toLowerCase());
    }

}
