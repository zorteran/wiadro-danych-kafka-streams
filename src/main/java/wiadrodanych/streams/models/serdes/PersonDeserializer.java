package wiadrodanych.streams.models.serdes;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Deserializer;
import wiadrodanych.streams.models.Person;

import java.nio.charset.Charset;
import java.util.Map;

public class PersonDeserializer implements Deserializer<Person> {
    private static final Charset CHARSET = Charset.forName("UTF-8");
    static private Gson gson = new Gson();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Person deserialize(String s, byte[] bytes) {
            String person = new String(bytes, CHARSET);
            return gson.fromJson(person, Person.class);
    }

    @Override
    public void close() {

    }
}
