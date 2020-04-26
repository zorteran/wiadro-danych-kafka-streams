package wiadrodanych.streams;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import sun.net.www.content.text.Generic;
import wiadrodanych.streams.models.InputZtmRecord;
import wiadrodanych.streams.models.OutputZtmRecord;
import wiadrodanych.streams.models.serdes.GenericDeserializer;
import wiadrodanych.streams.models.serdes.GenericSerializer;
import wiadrodanych.streams.models.serdes.InputZtmRecordDeserializer;
import wiadrodanych.streams.models.serdes.InputZtmRecordSerializer;

import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ZtmStreamTest {

    TopologyTestDriver testDriver;

    StringSerializer stringSerializer = new StringSerializer();
    StringDeserializer stringDeserializer = new StringDeserializer();
    InputZtmRecordSerializer inputZtmRecordSerializer = new InputZtmRecordSerializer();
    InputZtmRecordDeserializer inputZtmRecordDeserializer = new InputZtmRecordDeserializer();
    GenericSerializer outputZtmRecordSerializer = new GenericSerializer();
    GenericDeserializer<OutputZtmRecord> outputZtmRecordDeserializer = new GenericDeserializer<>();
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, OutputZtmRecord> outputTopic;

    @Before
    public void prepareTopologyTestDriver() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wiaderko-ztm-stream-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "doesnt-matter:1337");

        ZtmStream ztmStream = new ZtmStream();
        Topology topology = ztmStream.createTopology();
        testDriver = new TopologyTestDriver(topology, config);
        inputTopic = testDriver.createInputTopic(ZtmStream.INPUT_TOPIC, stringSerializer, stringSerializer);
        outputTopic = testDriver.createOutputTopic(ZtmStream.OUTPUT_TOPIC, stringDeserializer, outputZtmRecordDeserializer);
    }

    @After
    public void closeTestDriver() {
        testDriver.close();
    }
    @Test
    public void streamFiltersOldRecords(){
        String firstRecord = "{\"Lines\": \"108\", \"Lon\": 21.076998, \"VehicleNumber\": \"1037\", \"Time\": \"2020-04-25 20:16:53\", \"Lat\": 52.200958, \"Brigade\": \"51\"}";
        String secondRecord = "{\"Lines\": \"108\", \"Lon\": 21.076998, \"VehicleNumber\": \"1037\", \"Time\": \"2020-04-25 20:16:53\", \"Lat\": 52.200958, \"Brigade\": \"51\"}";
        inputTopic.pipeInput("1037", firstRecord);
        inputTopic.pipeInput("1037", secondRecord);
        List<OutputZtmRecord> output = outputTopic.readValuesToList();
        Assert.assertEquals(1, output.size());
    }
    @Test
    public void streamComputesSpeedDistanceAngle(){
        String firstRecord = "{\"Lines\": \"108\", \"Lon\": 21.076998, \"VehicleNumber\": \"1037\", \"Time\": \"2020-04-25 20:16:53\", \"Lat\": 52.200958, \"Brigade\": \"51\"}";
        String secondRecord = "{\"Lines\": \"108\", \"Lon\": 21.078823, \"VehicleNumber\": \"1037\", \"Time\": \"2020-04-25 20:17:34\", \"Lat\": 52.199871, \"Brigade\": \"51\"}";
        inputTopic.pipeInput("1037", firstRecord);
        inputTopic.pipeInput("1037", secondRecord);
        List<OutputZtmRecord> output = outputTopic.readValuesToList();
        Assert.assertEquals(1, output.size());
    }

}
