package oet.wouter.sandwich;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class MyToWire<T> implements Serializer<T>, Deserializer<T> {
    private final Class<T> clazz;

    public MyToWire(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        if(bytes == null)
            return null;
        return new Gson().fromJson(new String(bytes), clazz);
    }

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, T t) {
        return new Gson().toJson(t).getBytes();
    }

    @Override
    public void close() {

    }
}
