package wiadrodanych.streams.models.serdes;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;
import wiadrodanych.streams.models.ZtmRecord;

import java.nio.charset.Charset;
import java.util.Map;

public class ZtmRecordDeserializer implements Deserializer<ZtmRecord> {

    private static final Charset CHARSET = Charset.forName("UTF-8");
    static private Gson gson = new GsonBuilder()
            .create();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public ZtmRecord deserialize(String s, byte[] bytes) {
        String person = new String(bytes, CHARSET);
        return gson.fromJson(person, ZtmRecord.class);
    }

    @Override
    public void close() {

    }
}
