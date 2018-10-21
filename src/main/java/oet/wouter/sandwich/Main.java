package oet.wouter.sandwich;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.serialization.Serdes.Integer;
import static org.apache.kafka.common.serialization.Serdes.String;

public class Main {

    public static final String SOLD_SANDWICHES_TOPIC = "soldSandwiches";
    private static final String TIMED_SANDWICHES_TOPIC = "timed-sandwiches";
    private static final String ENRICHED_SANDWICHES_TOPIC = "enriched-sandwiches";
    private static final String LOCATIONS_TOPIC = "locations";
    private static final String COUNTED_SANDWICHES_TOPIC = "counted-sandwiches";

    private static final int BOUND = 10;

    private static final Serde<Location> LOCATION_SERDE = Util.getSerde(Location.class);
    static final Serde<SoldSandwich> SOLDSANDWICH_SERDE = Util.getSerde(SoldSandwich.class);

    public static void main(String[] args) throws IOException {
        runSandwichProducer();
        runLocationUpdater();
        runEnricher();
        runWindowedCounter();
        runCounted();

        System.out.println("Application initialized.");
    }

    private static void runCounted() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        Util.buildStream(streamsBuilder)
                .map(Util.mapToProperty(SoldSandwich::getName))
                .groupByKey(Serialized.with(String(), SOLDSANDWICH_SERDE))
                .count()
                .toStream()
                .map((key, value) -> new KeyValue<>(key, "Sold " + value + " of type " + key))
                .to(COUNTED_SANDWICHES_TOPIC, Produced.with(String(), String()));

        Util.createKafkaStream(streamsBuilder);
    }

    private static void runWindowedCounter() {
        StreamsBuilder builder = new StreamsBuilder();
        Util.buildStream(builder)
                .map(Util.mapToProperty(SoldSandwich::getName))
                .groupByKey(Serialized.with(String(), SOLDSANDWICH_SERDE))
                .windowedBy(TimeWindows.of(TimeUnit.MINUTES.toMillis(1)))
                .count()
                .toStream()
                .map(Util.mapWindowed())
                .to(TIMED_SANDWICHES_TOPIC, Produced.with(String(), String()));

        Util.createKafkaStream(builder);

    }

    private static void runEnricher() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KTable<Integer, Location> locations = streamsBuilder.table("locations", Consumed.with(Integer(), LOCATION_SERDE));

        ValueJoiner<SoldSandwich, Location, String> joiner = (event, location) -> "The sandwich " + event.getName() + " was sold at " + location.getStreet() + " (" + location.getId() + ")  `";

        Joined<Integer, SoldSandwich, Location> joined = Joined.with(Integer(), SOLDSANDWICH_SERDE, LOCATION_SERDE);

        Util.buildStream(streamsBuilder)
                .map((k, v) -> new KeyValue<>(v.getLocationId(), v))
                .join(locations, joiner, joined)
                .to(ENRICHED_SANDWICHES_TOPIC, Produced.with(Integer(), String()));

        Util.createKafkaStream(streamsBuilder);
    }


    private static void runSandwichProducer() {
        new Thread(() -> {
            boolean running = true;

            List<String> names = Data.getNames();

            Random random = new Random();
            KafkaProducer<String, SoldSandwich> producer = new KafkaProducer<>(Util.getConfig(), String().serializer(), new MyToWire<>(SoldSandwich.class));

            while (running) {
                SoldSandwich sandwich = new SoldSandwich(false, names.get(random.nextInt(names.size())), random.nextInt(BOUND));
                producer.send(new ProducerRecord<>(SOLD_SANDWICHES_TOPIC, UUID.randomUUID().toString(), sandwich));
                try {
                    Thread.sleep(random.nextInt(200));
                } catch (InterruptedException e) {
                    running = false;
                }
            }
        }).start();
    }

    private static void runLocationUpdater() {
        new Thread(() -> {
            boolean running = true;

            List<String> streets = Data.getStreets();
            KafkaProducer<Integer, Location> producer = new KafkaProducer<>(Util.getConfig(), Integer().serializer(), new MyToWire<>(Location.class));
            Random random = new Random();

            while (running) {
                int id = random.nextInt(BOUND);
                producer.send(new ProducerRecord<>(LOCATIONS_TOPIC, id, new Location(id, streets.get(random.nextInt(streets.size())))));

                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    running = false;
                }
            }
        }).start();
    }
}

