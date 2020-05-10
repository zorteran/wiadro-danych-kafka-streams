package wiadrodanych.streams;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import wiadrodanych.streams.models.ZtmRecord;
import wiadrodanych.streams.models.serdes.*;

import java.util.List;
import java.util.Properties;

public class ZtmStreamTest {

    TopologyTestDriver testDriver;

    StringSerializer stringSerializer = new StringSerializer();
    StringDeserializer stringDeserializer = new StringDeserializer();
    ZtmRecordDeserializer ztmRecordDeserializer = new ZtmRecordDeserializer();
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, ZtmRecord> outputTopic;

    @Before
    public void prepareTopologyTestDriver() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wiaderko-ztm-stream-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "doesnt-matter:1337");

        ZtmStream ztmStream = new ZtmStream();
        Topology topology = ztmStream.createTopology();
        testDriver = new TopologyTestDriver(topology, config);
        inputTopic = testDriver.createInputTopic(ztmStream.inputTopic, stringSerializer, stringSerializer);
        outputTopic = testDriver.createOutputTopic(ztmStream.outputTopic, stringDeserializer, ztmRecordDeserializer);

    }

    @After
    public void closeTestDriver() {
        testDriver.close();
    }

    @Test
    public void streamDropsSameRecords() {
        String firstRecord = "{\"Lines\": \"108\", \"Lon\": 21.076998, \"VehicleNumber\": \"1037\", \"Time\": \"2020-04-25 20:16:53\", \"Lat\": 52.200958, \"Brigade\": \"51\"}";
        String secondRecord = "{\"Lines\": \"108\", \"Lon\": 21.076998, \"VehicleNumber\": \"1037\", \"Time\": \"2020-04-25 20:16:53\", \"Lat\": 52.200958, \"Brigade\": \"51\"}";
        inputTopic.pipeInput("1037", firstRecord);
        inputTopic.pipeInput("1037", secondRecord);
        List<ZtmRecord> output = outputTopic.readValuesToList();
        Assert.assertEquals(1, output.size());
    }

    @Test
    public void streamDropsOldRecords() {
        String firstRecord = "{\"Lines\": \"108\", \"Lon\": 21.076998, \"VehicleNumber\": \"1037\", \"Time\": \"2020-04-25 20:17:53\", \"Lat\": 52.200958, \"Brigade\": \"51\"}";
        String secondRecord = "{\"Lines\": \"108\", \"Lon\": 21.076998, \"VehicleNumber\": \"1037\", \"Time\": \"2020-04-25 20:16:53\", \"Lat\": 52.200958, \"Brigade\": \"51\"}";
        inputTopic.pipeInput("1037", firstRecord);
        inputTopic.pipeInput("1037", secondRecord);
        List<ZtmRecord> output = outputTopic.readValuesToList();
        Assert.assertEquals(1, output.size());
    }

    @Test
    public void streamComputesAreCorrectly() {
        String firstRecord = "{\"Lines\": \"108\", \"Lon\": 21.076998, \"VehicleNumber\": \"1037\", \"Time\": \"2020-04-25 20:16:53\", \"Lat\": 52.200958, \"Brigade\": \"51\"}";
        String secondRecord = "{\"Lines\": \"108\", \"Lon\": 21.078823, \"VehicleNumber\": \"1037\", \"Time\": \"2020-04-25 20:17:34\", \"Lat\": 52.199871, \"Brigade\": \"51\"}";
        inputTopic.pipeInput("1037", firstRecord);
        inputTopic.pipeInput("1037", secondRecord);
        List<ZtmRecord> output = outputTopic.readValuesToList();
        ZtmRecord record = output.get(1);
        Assert.assertTrue(record.bearing > 0);
        Assert.assertTrue(record.speed > 0);
        Assert.assertTrue(record.distance > 0);
        Assert.assertEquals(0.1734, record.distance, 0.0001);
        Assert.assertEquals(134, record.bearing, 1);
        Assert.assertEquals(15.227, record.speed, 0.001);
    }

}
