package wiadrodanych.streams.models.serdes;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.Charset;
import java.util.Map;

public class GenericSerializer implements Serializer {

    private static final Charset CHARSET = Charset.forName("UTF-8");
    static private Gson gson = new Gson();
    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String s, Object o) {
        String object = gson.toJson(o);
        // Return the bytes from the String 'line'
        return object.getBytes(CHARSET);
    }

    @Override
    public void close() {

    }
}
