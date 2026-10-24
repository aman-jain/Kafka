# Understanding Resiliency in Kafka

## Purpose

This readme details out the different configurations and design patterns to use in order to make Kafka Resilient.
This readme does not dive into working of Kafka and it is expected that the reader knows the basic working of Kafka.
In order to make working with Kafka resilient, we need to make configurations + implement design patters in three core components of Kafka resilient viz: the Broker, the Producer and the Consumer. 

## Broker

The borker is responsible for multiple core aspects such as leader election between partitions, ensuring all partitions (or configured partitions) receive the copy of the message for replication, order guarantees etc.

The basic building block for Kafka is a partition. Each topic is broken down into partitions and each partition has its own disk to persist the data. Each partition can in turn have multiple replicas out of which one of them is elected the leader. While configuring your Kafka cluster you can select the number of replicas for each partition. Each partition that is not a leader tries to by in-sync with the the leader. 

A partition is declared to be out of sync if it does not send the heart beat message to KRaft or Zookeeper OR does not get the latest message from the leader.

### Replication

Selecting the number of replicas depend on several factors for example:

#### Availability

The more number of replicas that we have the better the availability of the cluster. 

#### Durability

The more number of replicas we have, the better durability of the system. In-case partitions go down- we do not have to worry about data loss. 

#### Throughput

Replicas introduce the amount of inter-borker traffic. Therefore more replicas would require more netowrk traffic and therefore it is important for planning for capacity in this case.

#### End-end latency

Increasing the number of replicas would also increase the end to end latency. If a particular message is going to be replicated on 3 replicas, it would take more time than replicating the data in just 1 replica.

#### Cost

Additional partitions would incurr high costs.

`replication.factor` is the top level configuation for specifying the number of replications for each partition.

There is also another configuration `min.sync.replicas` which means that for a message to be successfully declared as persisted in Kafka out of X replicas the message needs to be persisted with Y replicas (where X > Y). This works in conjencture with `acks` configuration on the Producer side. If the `acks` configuration is set to `all` then `min.sync.replicas` determines how many replicas need to have this message commited. 

### Leader Selection

Another configuration provided by Kafka is leader election. We can either choose a `clean`  leader or an `unclean` leader. In case of a leader crashing, one of the other replicas becomes a leader. At this point in time, we can either have one the in-sync partitions (`clean`) become the leader or out-of-sync partition (`unclean`) become a leader. 

There are pros and cons on using `clean` vs `unclean` leader. In a case where all replicas are out of sync, if we select `clean` configuration, then we will have to wait till the original leader comes back online OR the one of the out-of-sync replica becomes in-sync. At this point in time- the broker will stop accepting any new requests and start emitting an execption to the producer. On the other hand, if we opt for using `unclean` leader then we may elect a replca that does not have all the messages and there might be loss of messages. Based on our use-case, we need to determine which option to select.

## Producer

A prodcuer's is responsible for sending a message to Kafka and ensuring that it got consumed by Kafka topic. A simple producer will send the message and then wait for acknowledgement. There are few things that can go wrong here. 

1. A Producer can send the message and then crash without getting the ack.
2. A Producer can get the ack and then crash without registering the ack and upon recovering send back a duplicate message.
3. The Producer does not handle transient issues due to unavailability of the leader or loss of replicas.


### Solutions


#### Async vs Sync

A producer can wait for a ack message back from the Broker either synchronously or asynchronously. In-case sync method is implemented then the producer will wait to send out the next message causing latency. Depending on how busy the Kafka cluster is, brokers can take anywhere from 2 ms to a few seconds to respond to produce requests. If you send messages synchronously, the sending thread will spend this time waiting and doing nothing else, not even sending additional messages. This leads to very poor performance, and as a result, synchronous sends are usually not used in production applications

For using async handler, you need to pass a call back method to the send() method to get back either the successful message or the error back.

#### Handle Exceptions

When sending a message to Kafka, the producer can receive either retryable or non-retryable error. In case of a retryable error, the producer should have a mechanism to retry sending the messages for n-times with an expodential backoff strategy. In case of non-retryable errors, the producer should implement sending the records to a dead-letter queue (DLQ).

#### Use different configuration properties

Kafka provides different configurations to make producers more reliant and performant. 

##### linger .ms

This configuration allows Kafka to batch the requests. For example, if `linger.ms = 5ms` then Kafka will wait for 5ms, batch all the requests and then send it over. Using this property will depend upon the usage of your application. If your application is heavily ingesting data then setting `linger.ms` to a higher value makes sense as it will save the on the request time for each message. On the other hand, if the your application does not have a heavy producer, then setting this property will introduce latency.

##### batch.size

This configuration goes hand in hand with `linger.ms`. Using both of these properties you can control the amount of data that can be batched before sending to Kafka. Both these propperties operate in an 'OR' condition - which means if both these properties are set- whichever property gets satisfied first will result in releasing the batch. 

#### max.in.flight.requests.per.connection

This configuration allows Kafka to send X number of batches in parallel without waiting for one to complete. Thus, you increase Kafkas performance by first batching individual requests then enhance it further by fanning the batches out.

if `max.in.flight.requests.per.connection = 2` then Kafka will send over 2 batches without first batch to complete the cycle.
This property has to be used in conjenction with `enable.idempotent` property.

##### enable.idempotent

For each message sent to Kafka, the producer tags it with a producer id and a sequence number. This sequence number is handy for maintaining order when sending multiple batches and in-case there is a failure in one of the batches. For example, if the producer is sending Batch A and Batch B and Batch A fails then producer will again try to resend Batch A. Using this sequence number, Kafka is able to determine that Batch A needs to be delivered before Batch B. 

Let's say that Batch A was successfully sent over but the ack was lost. The producer will think that it needs to retry sending Batch A again and here is where if `enable.idempotency` is set to true, Kafka will understand that it has received a duplicate message and reject/ignore that message. 

These 2 properties give us:

```
    Multiple requests in flight
                +
          sequence numbers
                +
          duplicate detection
                +
            retries
                ↓
      ordered, duplicate-free
         producer writes
```



##### Retries

The Kafka producer internally performs retries in-case it does not get an ack back from the broker. To control the number of retires there are few properties:

`delivery.timeout.ms`: This property controls the total time it should take for the message to get replicated- including retries, and saving it in Kafka. This is the outer budget for the whole send: `delivery.timeout.ms` should be >= `request.timeout.ms` + `linger.ms`, since it has to contain at least one full request attempt plus any time the record spends waiting in a batch.

`request.timeout.ms`: Time to wait for getting a response from the broker. `request.timeout.ms` should be < `delivery.timeout.ms`.

`retries`: The number of retries the producer will perform to resend the message. Since Kafka 2.1, this defaults to `Integer.MAX_VALUE`, which means `delivery.timeout.ms` is effectively what bounds the retries, not `retries` itself- the producer just keeps retrying until the delivery timeout is reached.

`retry.backoff.ms`: Delay before the first retry attempt.

`retry.backoff.max.ms`: Since Kafka 3.7, the producer grows the backoff exponentially (with jitter) starting at `retry.backoff.ms` and capping at this value, so internal retries already follow an exponential backoff without any application code.

`delivery.timeout.ms` >= `request.timeout.ms` + `linger.ms`

#### Acks

As discussed earlier in the broker section, `acks` determine how many replicas should the message be saved before sending back a successful commit. 

#### Use Outbox pattern

Apart from setting the properties on Kafka producer that handles a lot of complexity for us, we do need to take care of 

1. Handling retryable exceptions from application side.
2. Ensuring that we processed all records and sent all records to Kafka.

For ensuring that all records are processed by Kafka:

a. Save the record in the database.
b. Save the event in the database.
c. Commit these records in database.
d. Poll messages not sent to Kafka. Send the message to Kafka.
e. Update the event as sent to Kafka.

## Consumer 

### Problems that can occur at a consumer side:
1. Consumer can crash after receiving the message.
2. Consumer can process the message and then crash.
3. Consumer is not able to process the message.

### Solutions

The first configuration that we have for consumers is `group.id`. All consumers that have the same `group.id` belong to the same consumer group. This means that Kafka will ensure that consumers within the same group are allocated different partitions of the topic. 

The other configuration that we have is `auto.commit`. Auto commit allows you to commit the messages on your behalf. The problem with this setting is that as a consumer you lose control incase the Consumer commits the message before processing it.

As a golden rule- you should always commit the messages after you have processed them. 

For ensuring that all records are consumed correctly, you can use the outbox pattern for the same:
1. poll() for data.
2. Check in the DB if this message was already processed.
3. If the message is already processed, then commit to Kafka- this helps with idempotency check.
4. If the message is not processed then start a transaction to save the message and the event in one commit.

The other two properties that can be used to control the Kafka consumer is `max.poll.interval.ms` and `max.poll.records`.

`max.poll.records` specifies the total number of records that the consumer can fetch when poll() is called.

`max.poll.interval.ms` specifies the time in which the consumer should call Kafka call for fresh new batch.

If the Kafka consumer does not poll() within the `max.poll.internal.ms` then Kafka deems the consumer as dead. This would initiate rebalancing of the consumers.

For processing invalid messages, you can retry processing the messages with expodential backoff, with ultimately sending the messages to the DLQ. 