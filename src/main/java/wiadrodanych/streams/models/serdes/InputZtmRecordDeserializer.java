package wiadrodanych.streams.models.serdes;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;
import wiadrodanych.streams.models.InputZtmRecord;

import java.nio.charset.Charset;
import java.util.Map;

public class InputZtmRecordDeserializer implements Deserializer<InputZtmRecord> {
    private static final Charset CHARSET = Charset.forName("UTF-8");
    static private Gson gson = new GsonBuilder()
            .setDateFormat("yyyy-MM-dd HH:mm:ss")
            .create();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public InputZtmRecord deserialize(String s, byte[] bytes) {
            String person = new String(bytes, CHARSET);
            return gson.fromJson(person, InputZtmRecord.class);
    }

    @Override
    public void close() {

    }
}
