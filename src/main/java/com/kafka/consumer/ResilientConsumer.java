package com.kafka.consumer;


import com.kafka.data.DataStorage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ResilientConsumer {
    private static final long POLL_DURATION = 1000;
    private final KafkaConsumer<String, String> consumer;

    public ResilientConsumer(Properties properties, String topic){
        this.consumer = new KafkaConsumer<String, String>(properties);
        this.consumer.subscribe(Collections.singletonList(topic));

    }
    public void poll(){

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(POLL_DURATION));
        for (ConsumerRecord<String, String> record : records) {
            //first check if the record exists in the record
            boolean isRecordAlreadyRead = DataStorage.isRecordConsumed(record.key());
            if (isRecordAlreadyRead){
                //if record is already read then continue
                continue;
            }
            System.out.printf("Received record(key=%s, value=%s) from partition=%d, offset=%d%n",
                    record.key(), record.value(), record.partition(), record.offset());
            DataStorage.storeConsumerRecord(record.key(),record.value());
            DataStorage.storeEventsConsumed(record.key(),record.value());

        }
        consumer.commitAsync();

    }
}
