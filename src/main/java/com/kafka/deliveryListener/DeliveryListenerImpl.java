package com.kafka.deliveryListener;

import com.kafka.data.DataStorage;

public class DeliveryListenerImpl implements DeliveryListener {

    @Override
    public void onDelivered(String key) {
        System.out.println("Message Successfully Delivered:"+key);
        DataStorage.deleteEvent(key);
    }

    @Override
    public void onDeadLettered(String key, Exception lastError, String service) {
        System.out.println("Message Unsuccessfully Delivered:"+key);
        //here we need to implement sending the data to a simulated dead-letter queue.
        DataStorage.storeDLQ(key, service);
        DataStorage.deleteEvent(key);
    }
}
