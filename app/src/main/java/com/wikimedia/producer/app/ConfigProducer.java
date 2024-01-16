package com.wikimedia.producer.app;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

public class ConfigProducer {

    public static Map<String, Object> initPropsProducer() {

        String sever = "localhost:9092";

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, sever);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    public static KafkaProducer<String, String> createConfigProducer(Map<String, Object> props) {

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(initPropsProducer());
        if (props != null) {
            initPropsProducer().putAll(props);
        }
        return kafkaProducer;

    }

}
