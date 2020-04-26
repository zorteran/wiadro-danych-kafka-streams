package wiadrodanych.streams.models.serdes;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;
import wiadrodanych.streams.models.InputZtmRecord;

import java.lang.reflect.ParameterizedType;
import java.nio.charset.Charset;
import java.util.Map;

public class GenericDeserializer<T> implements Deserializer<T> {
    private Class<T> typeOfT;

    @SuppressWarnings("unchecked")
    public GenericDeserializer() {
        this.typeOfT = (Class<T>)getClass().getGenericSuperclass();
    }

    private static final Charset CHARSET = Charset.forName("UTF-8");
    static private Gson gson = new GsonBuilder()
            .create();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public T deserialize(String s, byte[] bytes) {
            String person = new String(bytes, CHARSET);
            return gson.fromJson(person, typeOfT);
    }

    @Override
    public void close() {

    }
}
