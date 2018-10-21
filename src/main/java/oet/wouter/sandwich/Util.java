package oet.wouter.sandwich;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Windowed;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import static org.apache.kafka.common.serialization.Serdes.String;
import static org.apache.kafka.common.serialization.Serdes.serdeFrom;

public class Util {

    public static void createKafkaStream(StreamsBuilder streamsBuilder) {
        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), getStreamsConfig());
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    /**
     * A util function for repartitioning based on a property of the value of a message. The new key will be
     * the property and the new value will be the same as the old one.
     *
     * E.g.
     * <pre>
     *     stream() // of SoldSandwich
     *        .map(mapToProperty(SoldSandwich::getName))
     * </pre>
     * @param func which convert the value to the key. Usually Message::getSomeProperty
     * @param <K> the old key
     * @param <V> the old value
     * @param <NK> the new key
     * @return a KeyValueMapper
     */
    static <K, V, NK> KeyValueMapper<K, V, KeyValue<NK, V>> mapToProperty(Function<V, NK> func) {
        return (k, v) -> new KeyValue<>(func.apply(v), v);
    }

    static KeyValueMapper<Windowed<String>, Long, KeyValue<String, String>> mapWindowed() {
        return (key, value) -> {
            LocalDateTime start = LocalDateTime.ofInstant(Instant.ofEpochMilli(key.window().start()), ZoneId.systemDefault());
            LocalDateTime end = LocalDateTime.ofInstant(Instant.ofEpochMilli(key.window().end()), ZoneId.systemDefault());
            String formattedStart = start.toString();
            String formattedEnd = end.toString();
            return new KeyValue<>(null, "From " + formattedStart + " until " + formattedEnd + " we sold " + value + " sandwiches of type " + key.key());
        };
    }

    static Map<String, Object> getConfig() {
        return Map.of(
                StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString(),
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"
        );
    }

    private static StreamsConfig getStreamsConfig() {
        return new StreamsConfig(Util.getConfig());
    }

    static <T> Serde<T> getSerde(Class<T> clazz) {
        MyToWire<T> instance = new MyToWire<>(clazz);
        return serdeFrom(instance, instance);
    }

    public static KStream<String, SoldSandwich> buildStream(StreamsBuilder streamsBuilder) {
        return streamsBuilder.stream(Main.SOLD_SANDWICHES_TOPIC, Consumed.with(String(), Main.SOLDSANDWICH_SERDE, null, Topology.AutoOffsetReset.LATEST));
    }
}
