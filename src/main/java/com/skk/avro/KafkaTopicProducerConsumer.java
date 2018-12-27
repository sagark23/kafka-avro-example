
package com.skk.avro;


import static com.skk.avro.KafkaTopicSchemaTool.ADD;
import static com.skk.avro.KafkaTopicSchemaTool.CONSUME;
import static com.skk.avro.KafkaTopicSchemaTool.PRODUCE;

import com.skk.avro.compression.KafkaSpecificRecordDeserializer;
import com.skk.avro.compression.KafkaSpecificRecordSerializer;
import com.skk.avro.schema.KafkaTopicSchemaProvider;
import com.skk.avro.schema.SchemaUtils;
import com.skk.avroserialization.User;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;

/**
 * Kafka topic store usage main class
 * @author sagarkhandelwal23
 */
public class KafkaTopicProducerConsumer {

    private static final String KAFKA_CLUSTER = "localhost:9092,localhost:9092,localhost:9094"; //System.getProperty("INTEGRATION_TEST_CLUSTER");
    private static final String TOPIC = "avro-softmar";
    public static final String SCHEMAPROVIDER_TOPIC_NAME = "_com_skk_route_schemaprovider";

    /*
     kafka-topics --create --topic _com_skk_route_schemaprovider --partitions 1 --replication-factor 3 --config min.insync.replicas=2  --config retention.ms=-1 --config retention.bytes=-1 --zookeeper $(hostname):2181
     kafka-topics --create --topic avro-example                  --partitions 3 --replication-factor 3 --config min.insync.replicas=2 --zookeeper $(hostname):2181
    */

    public void add() throws Exception {
        KafkaTopicSchemaTool.main("--add", "--name", "user", "--version", "2",
            "--schema-file", "src/main/avro/user_v2_5.avsc", "--servers", KAFKA_CLUSTER, "--topic",
            SCHEMAPROVIDER_TOPIC_NAME);
    }

    public void _add() throws Exception {
        KafkaTopicSchemaTool.main("--add", "--name", "versioned_schema", "--version", "1",
            "--schema-file", "src/main/avro/versioned_schema.avsc", "--servers", KAFKA_CLUSTER, "--topic",
            SCHEMAPROVIDER_TOPIC_NAME);
    }

    public static void main(String... args) throws IOException {
        KafkaTopicProducerConsumer kafkaTopicProducerConsumer = new KafkaTopicProducerConsumer();
        OptionParser parser = new OptionParser();
        parser.accepts(ADD);
        parser.accepts(PRODUCE);
        parser.accepts(CONSUME);

        try {


            OptionSet options = parser.parse(args);

            if (options.has(ADD)) {
                kafkaTopicProducerConsumer.add();
            } else if (options.has(PRODUCE)) {
                kafkaTopicProducerConsumer.produce();
            }  else if (options.has(CONSUME)) {
                kafkaTopicProducerConsumer.consume();
            }
        } catch (Exception e) {
            e.printStackTrace();
            parser.printHelpOn(System.err);
        }

    }

    public void produce() throws Exception {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaSpecificRecordSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "-1");
        producerProps.put(KafkaSpecificRecordSerializer.VALUE_RECORD_CLASSNAME, User.class.getName());
        producerProps.put(SchemaUtils.SCHEMA_PROVIDER_FACTORY_CONFIG, KafkaTopicSchemaProvider.KafkaTopicSchemaProviderFactory.class.getName());
        producerProps.put(KafkaTopicSchemaProvider.SCHEMA_TOPIC_NAME_CONF, SCHEMAPROVIDER_TOPIC_NAME);
        producerProps.put(KafkaTopicSchemaProvider.SCHEMA_PROVIDER_CONF_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER);

        KafkaProducer<Integer, User> producer = new KafkaProducer<>(producerProps);

        for (User u : new User[] {
                new User("user1", "User, First", 0L),
                new User("user2", "User, Second", 10000L),
                new User("user3", "User, Name Third", 20000L)
        })
            producer.send(new ProducerRecord<>(TOPIC, u.getIdentifier().hashCode(), u)).get();
    }


    public void consume() {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaSpecificRecordDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, new Object().hashCode() + "");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(KafkaSpecificRecordDeserializer.VALUE_RECORD_CLASSNAME, User.class.getName());
        consumerProps.put(SchemaUtils.SCHEMA_PROVIDER_FACTORY_CONFIG, KafkaTopicSchemaProvider.KafkaTopicSchemaProviderFactory.class.getName());
        consumerProps.put(KafkaTopicSchemaProvider.SCHEMA_TOPIC_NAME_CONF, SCHEMAPROVIDER_TOPIC_NAME);
        consumerProps.put(KafkaTopicSchemaProvider.SCHEMA_PROVIDER_CONF_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER);

        KafkaConsumer<Integer, User> consumer = new KafkaConsumer<>(consumerProps);

        consumer.subscribe(Collections.singletonList(TOPIC));
        while(true) {
            consumer.poll(1000).forEach(r -> {
                User u = r.value();
                System.out.println(u);
            });
        }

    }
}
