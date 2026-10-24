package com.kafka.deliveryListener;

public interface DeliveryListener {
    void onDelivered(String key);

    void onDeadLettered(String key, Exception lastError, String Service);
}
