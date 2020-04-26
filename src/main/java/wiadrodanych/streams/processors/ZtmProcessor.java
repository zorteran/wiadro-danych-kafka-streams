package wiadrodanych.streams.processors;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import wiadrodanych.streams.models.OutputZtmRecord;

public class ZtmProcessor implements Processor<String, OutputZtmRecord> {

    private ProcessorContext context;
    private KeyValueStore<String, OutputZtmRecord> ztmRecordStore;


    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        ztmRecordStore = (KeyValueStore) context.getStateStore("ztmStore");
    }

    @Override
    public void process(String key, OutputZtmRecord record) {
        KeyValueIterator<String, OutputZtmRecord> iter = this.ztmRecordStore.all();
        while (iter.hasNext()) {
            KeyValue<String, OutputZtmRecord> entry = iter.next();
            //context.forward(entry.key, entry.value.toString());
        }
        iter.close();
        OutputZtmRecord previousRecord = ztmRecordStore.get(key);
        if (previousRecord == null) {
            ztmRecordStore.put(key, record);
        } else {
            if (previousRecord.time == record.time) {
                record.toBeDeleted = true;
                return;
            }
        }

        context.forward(key, record);
    }

    private double calculateDistance(double lat1, double lon1, double lat2, double lon2) {
        if ((lat1 == lat2) && (lon1 == lon2)) {
            return 0;
        } else {
            double theta = lon1 - lon2;
            double dist = Math.sin(Math.toRadians(lat1)) * Math.sin(Math.toRadians(lat2)) + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.cos(Math.toRadians(theta));
            dist = Math.acos(dist);
            dist = Math.toDegrees(dist);
            dist = dist * 60 * 1.1515;
            dist = dist * 1.609344;
            return (dist);
        }
    }

    @Override
    public void close() {

    }
}
