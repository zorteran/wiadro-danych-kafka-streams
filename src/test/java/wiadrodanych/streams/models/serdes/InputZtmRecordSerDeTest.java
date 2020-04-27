package wiadrodanych.streams.models.serdes;

import org.junit.Assert;
import org.junit.Test;
import wiadrodanych.streams.models.InputZtmRecord;
import wiadrodanych.streams.models.ZtmRecord;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;

public class InputZtmRecordSerDeTest {
    @Test
    public void ztmRecordSerializationWorks(){
        InputZtmRecordDeserializer deserializer = new InputZtmRecordDeserializer();

        String rawRecord = "{\"Lines\": \"204\", \"Lon\": 21.043399, \"VehicleNumber\": \"1042\", \"Time\": \"2020-04-24 21:14:34\", \"Lat\": 52.26617, \"Brigade\": \"04\"}";
        InputZtmRecord record =  deserializer.deserialize(null, rawRecord.getBytes());

        Assert.assertEquals("204",record.lines);
        Assert.assertEquals(21.043399,record.lon,0.0001);
        Assert.assertEquals(52.26617,record.lat,0.0001);
        Assert.assertEquals("04",record.brigade);
        Assert.assertEquals("1042",record.vehicleNumber);
        Assert.assertEquals("204",record.lines);
        Assert.assertEquals(Date.from(LocalDateTime.of(2020,04,24,21,14,34).atZone(ZoneOffset.systemDefault()).toInstant()),record.time);
    }
    @Test
    public void ztmRecordDeserializationDoesntThrowException(){
        InputZtmRecordSerializer serializer = new InputZtmRecordSerializer();
        Date someDate = new Date();
        InputZtmRecord record = new InputZtmRecord("100",123.321,321.123,"101","102",someDate);
        
        String serializedRecord = new String(serializer.serialize(null,record));
    }

    @Test
    public void inputZtmRecordToZtmRecordDeserializerWorks(){
        InputZtmRecordToZtmRecordDeserializer deserializer = new InputZtmRecordToZtmRecordDeserializer();

        String rawRecord = "{\"Lines\": \"204\", \"Lon\": 21.043399, \"VehicleNumber\": \"1042\", \"Time\": \"2020-04-24 21:14:34\", \"Lat\": 52.26617, \"Brigade\": \"04\"}";
        ZtmRecord record =  deserializer.deserialize(null, rawRecord.getBytes());

        Assert.assertEquals("204",record.lines);
        Assert.assertEquals(21.043399,record.lon,0.0001);
        Assert.assertEquals(52.26617,record.lat,0.0001);
        Assert.assertEquals("04",record.brigade);
        Assert.assertEquals("1042",record.vehicleNumber);
        Assert.assertEquals("204",record.lines);
    }
}
