package wiadrodanych.streams.models.serdes;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;
import wiadrodanych.streams.models.InputZtmRecord;
import wiadrodanych.streams.models.ZtmRecord;

import java.nio.charset.Charset;
import java.util.Map;

public class InputZtmRecordToZtmRecordDeserializer implements Deserializer<ZtmRecord> {
    private static final Charset CHARSET = Charset.forName("UTF-8");
    static private Gson gson = new GsonBuilder()
            .setDateFormat("yyyy-MM-dd HH:mm:ss")
            .create();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public ZtmRecord deserialize(String s, byte[] bytes) {
            String rawRecord = new String(bytes, CHARSET);
            InputZtmRecord inputRecord = gson.fromJson(rawRecord, InputZtmRecord.class);
            return new ZtmRecord(inputRecord);
    }

    @Override
    public void close() {

    }
}
