package wiadrodanych.streams.processors;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import wiadrodanych.streams.models.ZtmRecord;
import wiadrodanych.utils.GeoTools;

public class ZtmProcessor implements Processor<String, ZtmRecord> {
    public static final int SUSPICIOUS_SPEED = 120;
    private ProcessorContext context;
    private KeyValueStore<String, ZtmRecord> ztmRecordStore;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        ztmRecordStore = (KeyValueStore) context.getStateStore("ztmStore");
    }

    @Override
    public void process(String key, ZtmRecord record) {
        ZtmRecord previousRecord = ztmRecordStore.get(key);
        if (previousRecord == null) {
            ztmRecordStore.put(key, record);
            context.forward(key, record);
            return;
        }
        if (previousRecord.time.compareTo(record.time) >= 0) {
            return; // ignore old/same record
        }
        record = calculateRecord(previousRecord, record);
        if (record.speed > SUSPICIOUS_SPEED)
        {
            return; // probably measurement error
        }
        ztmRecordStore.put(key, record);
        context.forward(key, record);
    }

    private ZtmRecord calculateRecord(ZtmRecord previousRecord, ZtmRecord record) {
        double lat1 = previousRecord.lat;
        double lat2 = record.lat;
        double lon1 = previousRecord.lon;
        double lon2 = record.lon;
        record.distance = GeoTools.calculateDistanceInKilometers(lat1, lat2, lon1, lon2);
        record.bearing = GeoTools.calculateBearing(lat1, lat2, lon1, lon2);
        record.speed = GeoTools.calculateSpeed(record.distance,previousRecord.time, record.time);
        return record;
    }
    @Override
    public void close() {
    }
}
