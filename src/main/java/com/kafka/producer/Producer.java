package com.kafka.producer;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import com.kafka.data.DataStorage;

import com.kafka.deliveryListener.DeliveryListenerImpl;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Producer {

    private static final long LOOP_DELAY_MS = 500;

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // key and value serializer
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // acks this property is used to control the number of acknowledgments the producer receives from the broker.
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        // retries this property is used to control the number of times the producer will retry to send the message in case of failure.
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        // retry.backoff.ms this property is the starting backoff between internal retries.
        properties.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "100");
        // retry.backoff.max.ms this property caps the backoff. Since Kafka 3.7 the client
        // grows the wait exponentially (with jitter) from retry.backoff.ms up to this value,
        // so the producer's own retries are already an exponential backoff.
        properties.setProperty(ProducerConfig.RETRY_BACKOFF_MAX_MS_CONFIG, "1000");
        // linger.ms this property is used to control the time between batches.
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        // batch.size this property is used to control the size of the batch.
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        // delivery.timeout.ms this property is used to control the timeout for the delivery of the message.
        properties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "10000");
        // request.timeout.ms this property is used to control the timeout for the request.
        properties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "3000");
        // max.in.flight.requests.per.connection this property is used to control the maximum number of requests that can be in flight at a time.
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        // enable.idempotence this property is used to enable the idempotence for the producer. 
        // this property is used to ensure that the message is delivered exactly once.
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        // create a new Kafka producer. Once a record is delivered, or parked on the DLQ,
        // the outbox event is deleted so the next poll does not pick it up again.
        ResilientProducer resilientProducer = new ResilientProducer(properties, new DeliveryListenerImpl());

        while (true) {
            String key = "id_" + UUID.randomUUID().toString();
            String value = "message_" + UUID.randomUUID().toString();
            boolean isRecordStored = DataStorage.storeProducerRecord(key, value);
            boolean isEventStored = false;
            if (isRecordStored) {
                isEventStored = DataStorage.storeEvent(key, value);
            }
            if (isRecordStored && isEventStored) {
                // Copy the keys: delivery callbacks remove entries from another thread.
                Set<String> keysToDeliver = DataStorage.getEventsToDeliver();
                if (keysToDeliver != null) {
                    keysToDeliver = new HashSet<>(keysToDeliver);
                    for (String keyToDeliver : keysToDeliver) {
                        String valueToDeliver = DataStorage.getData(keyToDeliver);
                        ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", keyToDeliver, valueToDeliver);
                        resilientProducer.send(record);
                    }
                }
            }

            // Without this, the loop re-scans the outbox and re-sends every still
            // in-flight record on every iteration, long before their callbacks
            // return - flooding the topic with duplicates of the same key.
            try {
                Thread.sleep(LOOP_DELAY_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
