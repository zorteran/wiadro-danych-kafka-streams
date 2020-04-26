package wiadrodanych.streams.models.serdes;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;
import wiadrodanych.streams.models.OutputZtmRecord;

import java.nio.charset.Charset;
import java.util.Map;

public class ZtmRecordDeserializer implements Deserializer<OutputZtmRecord> {

    private static final Charset CHARSET = Charset.forName("UTF-8");
    static private Gson gson = new GsonBuilder()
            .create();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public OutputZtmRecord deserialize(String s, byte[] bytes) {
        String person = new String(bytes, CHARSET);
        return gson.fromJson(person, OutputZtmRecord.class);
    }

    @Override
    public void close() {

    }
}
