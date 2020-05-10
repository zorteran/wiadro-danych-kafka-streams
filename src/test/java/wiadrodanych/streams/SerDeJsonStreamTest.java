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
import wiadrodanych.streams.models.Person;
import wiadrodanych.streams.models.serdes.PersonDeserializer;
import wiadrodanych.streams.models.serdes.PersonSerializer;

import java.util.List;
import java.util.Properties;

public class SerDeJsonStreamTest {

    TopologyTestDriver testDriver;

    StringSerializer stringSerializer = new StringSerializer();
    StringDeserializer stringDeserializer = new StringDeserializer();
    PersonSerializer personSerializer = new PersonSerializer();
    PersonDeserializer personDeserializer = new PersonDeserializer();
    private TestInputTopic<String, Person> inputTopic;
    private TestOutputTopic<String, Person> outputTopic;

    @Before
    public void prepareTopologyTestDriver() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "SerDeJsonStreamTest");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "doesnt-matter:1337");

        SerDeJsonStream serDeJsonStream = new SerDeJsonStream();
        Topology topology = serDeJsonStream.createTopology();
        testDriver = new TopologyTestDriver(topology, config);
        inputTopic = testDriver.createInputTopic(SerDeJsonStream.INPUT_TOPIC, stringSerializer, personSerializer);
        outputTopic = testDriver.createOutputTopic(SerDeJsonStream.OUTPUT_TOPIC, stringDeserializer, personDeserializer);
    }

    @After
    public void closeTestDriver() {
        testDriver.close();
    }

    @Test
    public void personUnder18ShouldNotBeProcessed(){
        Person inputPerson1 = new Person("grzesiek", 17);
        Person inputPerson2 = new Person("ADAM", 20);
        inputTopic.pipeInput(inputPerson1);
        inputTopic.pipeInput(inputPerson2);
        List<Person> output = outputTopic.readValuesToList();
        Person outputPerson = output.stream().findFirst().get();
        Assert.assertTrue(output.size() == 1);
        Assert.assertEquals("Adam", outputPerson.name);
    }

    @Test(expected = StringIndexOutOfBoundsException.class)
    public void personWithEmptyNameShouldNotBeProcessed_ButThrowsException_(){
        Person inputPerson = new Person("", 17);
        inputTopic.pipeInput(inputPerson);
        Assert.assertTrue(outputTopic.isEmpty());
    }

}
