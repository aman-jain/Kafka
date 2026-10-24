package com.kafka.producer;

import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import com.kafka.deliveryListener.DeliveryListener;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RetriableException;

/**
 * A producer that retries application level delivery failures with an exponential
 * backoff, and parks anything it cannot deliver on a dead letter topic.
 *
 * Note that this sits *on top of* the retries the Kafka client already performs
 * internally (see retries / retry.backoff.ms / retry.backoff.max.ms). By the time a
 * callback here reports an exception, the client has already exhausted
 * delivery.timeout.ms, so these retries are the second line of defence.
 */
public class ResilientProducer implements AutoCloseable {

    /** Retries after the first attempt, so a record is attempted MAX_RETRIES + 1 times. */
    private static final int MAX_RETRIES = 3;
    /** Delay before the first retry. Doubles on every subsequent retry. */
    private static final long BASE_BACKOFF_MS = 1000;
    /** Ceiling for the backoff, so a long outage does not push retries hours apart. */
    private static final long MAX_BACKOFF_MS = 30_000;

    private final KafkaProducer<String, String> producer;
    private final ScheduledExecutorService retryScheduler;
    private final DeliveryListener listener;


    public ResilientProducer(Properties properties,  DeliveryListener listener) {
        this.producer = new KafkaProducer<>(properties);

        this.listener = listener;
        // Retries are scheduled off the Kafka I/O thread. Sleeping inside a callback
        // would block the Sender thread and stall every other in-flight batch.
        this.retryScheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "producer-retry-scheduler");
            thread.setDaemon(true);
            return thread;
        });
    }

    /**
     * Send a record, retrying retryable failures with an exponential backoff.
     * Returns immediately; delivery is reported on the listener.
     */
    public void send(ProducerRecord<String, String> record) {
        send(record, 0);
    }

    private void send(ProducerRecord<String, String> record, int attempt) {
        try {
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("Message sent successfully:" + record.key()
                            + " with offset:" + metadata.offset()
                            + " and partition: " + metadata.partition());
                    listener.onDelivered(record.key());
                    return;
                }
                System.out.println("Message sending failed for " + record.key()
                        + " on attempt " + (attempt + 1) + ": " + exception.getMessage());
                scheduleRetryOrDeadLetter(record, attempt, exception);
            });
        } catch (Exception exception) {
            // send() throws synchronously for serialization errors, a full buffer
            // (max.block.ms) or a closed producer, instead of going to the callback.
            System.out.println("Message could not be enqueued for " + record.key()
                    + " on attempt " + (attempt + 1) + ": " + exception.getMessage());
            scheduleRetryOrDeadLetter(record, attempt, exception);
        }
    }

    private void scheduleRetryOrDeadLetter(ProducerRecord<String, String> record, int attempt,
            Exception exception) {
        if (!isRetryable(exception)) {
            System.out.println("Non-retryable error for " + record.key() + ", not retrying");
            sendToDlq(record, exception);
            return;
        }
        if (attempt >= MAX_RETRIES) {
            System.out.println("Exhausted " + (MAX_RETRIES + 1) + " attempts for " + record.key());
            sendToDlq(record, exception);
            return;
        }
        long delay = backoffMillis(attempt);
        System.out.println("Retrying " + record.key() + " in " + delay + "ms");
        retryScheduler.schedule(() -> send(record, attempt + 1), delay, TimeUnit.MILLISECONDS);
    }

    /**
     * Kafka marks transient failures with the RetriableException interface: leader
     * elections, not enough in-sync replicas, timeouts, broker disconnects. Everything
     * else (a bad serializer, an oversized record, an unknown topic) will fail the same
     * way on every attempt, so it goes straight to the DLQ.
     */
    private boolean isRetryable(Exception exception) {
        return exception instanceof RetriableException;
    }

    /**
     * Exponential backoff with equal jitter: the delay doubles each attempt, and the
     * jitter stops a batch of records that failed together from retrying in lockstep
     * and hammering a broker that is still recovering.
     */
    private long backoffMillis(int attempt) {
        long exponential = Math.min(MAX_BACKOFF_MS, BASE_BACKOFF_MS * (1L << attempt));
        long half = exponential / 2;
        return half + ThreadLocalRandom.current().nextLong(half + 1);
    }

    private void sendToDlq(ProducerRecord<String, String> record, Exception lastError) {
        System.out.println("Failed to send the message:" + record.key() + " Sending it to DLQ");
        listener.onDeadLettered(record.key(), lastError, "producer");

    }

    @Override
    public void close() {
        retryScheduler.shutdown();
        try {
            // Give pending retries a chance to run before the producer goes away.
            if (!retryScheduler.awaitTermination(MAX_BACKOFF_MS, TimeUnit.MILLISECONDS)) {
                retryScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            retryScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        producer.close();
    }
}
