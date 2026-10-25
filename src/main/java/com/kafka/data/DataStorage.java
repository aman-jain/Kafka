package com.kafka.data;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The purpose of this class is to mimic storing the data in actual database or cache.
 * If we use this as-is in production, this will break because as we are storing all the data in memory,
 * in an event of a crash, all data will be lost.
 */
public class DataStorage {
    // Concurrent: the main loop writes these while producer delivery callbacks
    // remove delivered events from the Kafka I/O and retry threads.
    private static final Map<String, String> producerData = new ConcurrentHashMap<>();
    private static final Map<String, String> eventsToDeliver = new ConcurrentHashMap<>();
    private static final Map<String,String> dlq = new ConcurrentHashMap<>();
    private static final Map<String, String> consumerData = new ConcurrentHashMap<>();
    private static final Map<String, String> eventsConsumed = new ConcurrentHashMap<>();
    /**
     * Store a record in the database
     * @param key
     * @param value
     * @return true if the record is stored successfully, false otherwise
     */
    public static boolean storeProducerRecord(String key, String value) {
        System.out.println("Storing record in the database: " + key + " " + value);
        if (producerData.containsKey(key)) {
            return false;
        }
        producerData.put(key, value);
        return true;
    }

    /**
     * Store a record in the database
     * @param key
     * @param value
     * @return true if the record is stored successfully, false otherwise
     */
    public static boolean storeConsumerRecord(String key, String value) {
        System.out.println("Storing record in the database: " + key + " " + value);
        if (consumerData.containsKey(key)) {
            return false;
        }
        consumerData.put(key, value);
        return true;
    }

    /**
     * Store an event in the database
     * @param eventKey
     * @param value
     * @return true if the event is stored successfully, false otherwise
     */
    public static boolean storeEvent(String eventKey, String value) {
        System.out.println("Storing event in the database: " + eventKey + " " + value);
        if (eventsToDeliver.containsKey(eventKey)) {
            return false;
        }
        eventsToDeliver.put(eventKey, value);
        return true;
    }


    /**
     * Store an event in the database
     * @param eventKey
     * @param value
     * @return true if the event is stored successfully, false otherwise
     */
    public static boolean storeEventsConsumed(String eventKey, String value) {
        System.out.println("Storing event in the database: " + eventKey + " " + value);
        if (eventsConsumed.containsKey(eventKey)) {
            return false;
        }
        eventsConsumed.put(eventKey, value);
        return true;
    }

    /**
     * Get the events to deliver
     * @return the events to deliver
     */
    public static Set<String> getEventsToDeliver() {
        if (eventsToDeliver.isEmpty()) {
            return null;
        }
        return eventsToDeliver.keySet();
    }

    /**
     * Delete an event from the database
     * @param eventKey
     * @return true if the event is deleted successfully, false otherwise
     */
    public static boolean deleteEvent(String eventKey) {
        if (!eventsToDeliver.containsKey(eventKey)) {
            return false;
        }
        eventsToDeliver.remove(eventKey);
        return true;
    }

    /**
     * Get the data from the database
     * @param key
     * @return the data
     */
    public static String getData(String key) {
        return producerData.get(key);
    }

    /**
     * Stores the data in DLQ
     * @param key
     * @param service - here the service is either the producer or consumer.
     * @return
     */
    public static boolean storeDLQ(String key, String service){
        System.out.println("Storing event in the DLQ: " + key + " by" + service);
        if (dlq.containsKey(key)) {
            return false;
        }
        dlq.put(key, service);
        return true;
    }

    public static boolean isRecordConsumed(String key){
        System.out.println("Checking if key is already consumed: " + key);
        if (eventsConsumed.containsKey(key)) {
            return true;
        }
        return false;
    }
}
