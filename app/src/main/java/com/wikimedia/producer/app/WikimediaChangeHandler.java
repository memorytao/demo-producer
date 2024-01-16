package com.wikimedia.producer.app;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;

public class WikimediaChangeHandler implements EventHandler {

    KafkaProducer<String, String> kafkaProducer;
    String topic;

    private static final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());

    public WikimediaChangeHandler(KafkaProducer<String, String> producer, String topic) {
        this.kafkaProducer = producer;
        this.topic = topic;
    }

    @Override
    public void onOpen() {
        // TODO Auto-generated method stub
    }

    @Override
    public void onClosed() {
        // TODO Auto-generated method stub
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {
        // TODO Auto-generated method stub

        log.info(messageEvent.getData());        
        kafkaProducer.send(new ProducerRecord<String,String>(topic, messageEvent.getData()));
    }

    @Override
    public void onComment(String comment) throws Exception {
        // TODO Auto-generated method stub
    }

    @Override
    public void onError(Throwable t) {
        // TODO Auto-generated method stub
    }

}
