# Sandwich

My project to play with Apache Kafka and Apache Kafka Streams.

First run Apache Kafka with:
* docker run -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST="127.0.0.1" --env ADVERTISED_PORT=9092 spotify/kafka

Handy commando's to view the results:

* ./kafka-console-consumer.sh --topic soldSandwiches --bootstrap-server localhost:9092
* ./kafka-console-consumer.sh --topic timed-sandwiches --bootstrap-server localhost:9092
* ./kafka-console-consumer.sh --topic enriched-sandwiches --bootstrap-server localhost:9092
* ./kafka-console-consumer.sh --topic locations --bootstrap-server localhost:9092
* ./kafka-console-consumer.sh --topic counted-sandwiches --bootstrap-server localhost:9092

