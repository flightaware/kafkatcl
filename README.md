KafkaTcl, a Tcl interface to the Apache Kafka distributed messaging system
===

KafkaTcl provides a Tcl interface to the Kafka C language API "librdkafka".

Functionality
---

- Provides a natural Tcl interface
- Fast
- Asynchronous
- Thread safe
- Free!

License
---

Open source under the permissive Berkeley copyright, see file LICENSE

Requirements
---
Requires the Apache Kafka C/C++ library *librdkafka* be installed.  (https://github.com/edenhill/librdkafka)

Building
---

```sh
autoconf
configure
make
sudo make install
```

For FreeBSD, something like

```sh
./configure --with-tcl=/usr/local/lib/tcl8.6  --mandir=/usr/local/man --enable-symbols
```

Accessing from Tcl
---

```tcl
package require kafka
```

Overview
---

You've got your kafka object, which is like your master connection.

Using methods of your kafka object you create kafka handle objects.

Kafka handle objects are used to create topic producers and consumers.

Topic producer and consumer objects are used to produce and consume messages to kafka.

Or you can you the simple interface.


KafkaTcl commands
---

The kafkatcl command from which all others are created is **::kafka::kafka**.

* **kafka::kafka** **version**

 Returns the kafka version as a string, like 0.8.6.

* **kafka::kafka** **create** *command*

 Create a kafka object.  If *command* is **#auto** then an handle with a unique name such as **kafka_handle0** will be created for you.  Of course with that usage you'd want to capture the handle name in a variable or something.

```tcl
set kafka [::kafka::kafka create #auto]

$kafka logger callback klog

set consumer [$kafka consumer_creator #auto]
```

...or...

```tcl
::kafka::kafka create kafka]

kafka logger callback klog

kafka consumer_creator consumer
```

Methods of kafka interface object
---

* *$kafka* **config** *?key value? ...*

 If invoked without arguments, returns a list of the configuration properties and values suitable for passing to *array set* or whatever.

 If invoked with one or more pairs of property and value arguments, sets the value of each property-value pair into the configuration properties.

 Returns a Tcl error if it fails.

* *$kafka* **producer_creator** *cmdName*

 Create a kafkatcl producer handle object.  the producer handle object is used to establish communications with the kafka cluster and eventually create topic producer objects.

* *$kafka* **consumer_creator** *cmdName*

 Create a kafkatcl consumer handle object.  The consumer handle object is used to connect with the kafka cluster and eventually create topic consumer topics.

* *$kafka* **topic_config** *?key value? ...*

 If invoked without arguments returns a list of the default topic configuration properties and values.

 As with **config**, if invoked with one or more pairs of property and value arguments, sets the value of each property-value pair into the topic configuration properties.

* *$kafka* **partitioner** *partitionerName*

 Selects one of the rdkafka-provided partitioners.  The partitioner determines which partition a message should go in.  *random* selects the random partitioner (the default) which will cause produced messages to go into a random partition between zero and the number of partitions of the topic minus one.

 *consistent* uses a consistent hashing to map identical keys onto identical partitions.  The key must be specified when producing messages when the consistent partitioner has been selected.

* *$kafka* **delivery_report *option* *?args?*

* *$kafka* **delivery_report** **callback** *command*

 Invoke *command* when kafka cpp-driver delivery report callbacks are received.

 Data returned currently is the payload, partition and offset.

 Note that delivery report callbacks will only be performed for producer topics that are created after the master object has delivery reports configured.  If a topic is created and subsequently delivery report callbacks are set in the master object, the previously created topic will not receive delivery callbacks when payloads are produced to kafka.  This is also true for error and statistics callbacks.

* *$kafka* **delivery_report** **every** *count*

 Only perform the delivery report callback event in Tcl every *count* delivery reports received, default 1 for every report received to call back to Tcl.  If set to 100, for instance, the first and every hundredth delivery report callback received would invoke the callback routine.

 If set to 0, no delivery reports make it back to Tcl unless the **sample** option is invoked or a different delivery count is selected.

* *$kafka* **delivery_report** **sample**

 Sets that the next delivery report callback received will invoke the Tcl callback.  This is so that you could for instance obtain the offset periodically.

* *$kafka* **error_callback** *command*

 Invoke *command* when kafka cpp-driver error callbacks are received.

* *$kafka* **statistics_callback** *command*

 Invoke *command* when kafka cpp-driver statistics callbacks are received.

 The command will be invoked with one argument, which is the JSON provided by the stats callback.

 You will need to config *statistics.interval.ms* to the interval you want statistics called back at.

* *$handle* **logger** **syslog**

 Log kafka logger messages to the system log.

* *$handle* **logger** **stderr**

 Log kafka logger messages to stderr.

* *$handle* **logger** **callback** *command*

 Invoke the callback routine *command* when kafka logger messages are received.

 After specifying this, when logging messages are received, *command* will be invoked with one argument containing a list of key-value pairs containing "level", the log level as an integer (this may change), "facility", the facility as a string, and "message" followed by the log message.

 Logging callbacks may only be directed to one of the syslog, stderr or the callback command.  Changing to a new one ends the use of the previous one.


Methods of kafka consumer and producer handle object
---

* *$handle* **name**

 Return the kafka name of the handle.  You'll get something like **rdkafka#producer-0**.

* *$handle* **config** *?key value?*

 The kafka handle object inherits a copy of the topic config from the master object.

 **config** provides a way to examine and alter the handle's topic configuration properties.  (These properties are accessed when the producer/consumer objects are created.)

 If invoked without arguments returns a list of the topic configuration properties and values.

 If invoked with one or more pairs of property and value arguments, sets the value of each property-value pair into the topic configuration properties.

 When a topic-producing or topic-consuming object is created, the topic config is set from the corresponding handle object that created the producer or consumer object.

```
    $handle config compression.codec gzip
```

 *timeoutMS*, if present, specifies the minimum amount of time in milliseconds that the call will block waiting for events.  If no timeout is specified or 0 is specified as the timeout, the handle will be pulled nonblocking.

 If -1 is specified then Tcl will wait indefinitely for an event, which is probably not a good idea.

 We expect to further develop kafkatcl where polling will be done automatically within kafkatcl using Tcl event mechanisims.

* *$handle* **new_topic** *commandName* *topic*

 Creates a new topic consumer or producer object corresponding to the specified topic.

 If the handle was created with the **producer_creator** method then a topic producer object will be created.  If it was created with the **consumer_creator** method then a topic consumer object will be created.

* *$handle* **log_level** *level*

 Set the log level to one of **emerg**, **alert**, **crit**, **err**, **warning**, **notice**, **info**, or **debug**.

* *$handle* **add_brokers** *brokerList*

 Add a list of kafka brokers to the kafka handle object.

* *$handle* **create_queue** *command*

 Create a queue object named *command*.  If *command* is **#auto** then creates a unique command name such as *kafka_queue0*.

* *$handle* **output_queue_length**

 Return the current output queue length, i.e. the messages waiting to be sent to, or acknowledged by, the broker.

* *$handle* **info** **topics**

 Return a list of the topics defined on the kafka cluster.

* *$handle* **info** **brokers**

 Return a list of all the brokers defined on the connected kafka cluster.  Each element is the IP address of the broker followed by a colon character followed by the port number of the broker, for example *10.3.7.3:9092*.

* *$handle* **info** **partitions** *topic*

 Return the number of partitions defined for the specified topic.

* *$handle* **meta** **refresh**

 Refresh the metadata by reobtaining it from the server.

* *$handle* **meta** **print**

 Print the metadata.  For debugging only; doesn't go through the Tcl I/O system.

* *$handle* delete

 Delete the handle object, destroying the command.

Methods of kafka topic producer object
---

* *$topic* **produce** *partition* *payload* *?key?*

 Produce one message into the specified partition.  If there's an error, you get a Tcl error.  IF the partition is -1 then the unassigned partition is specified, indicating that kafka should partition using the configured or default partitioner.

 If *key* is specified then it's passed to the topic partitioner as well as sent to the broker and passed to the consumer.  That means the partitioning algorithm can use that to help pick the partition.  Also it's a value that can be sent through alongside the payload.

* *$topic* **produce_batch** *partition* *list-of-payload-key-lists*

 Produce a list of messages into the specified partition.  The list is a list of lists.  Each sublist must contain one or two elements.  If one element is present, it is the message payload.  If two are present, it is the payload and optional key.

* *$topic* **config** *?key value? ...*

 Works the same as **config** for consumer handle (topic-creating) objects.

* *$topic* **info** **topic**

Return the name of the topic.

* *$topic* **info** **partitions**

Return the number of partitions defined for the topic.

* *$topic* **info** **consistent_partition** *key*

Return the partition number that the Kafka consistent partitioner will return for the given key for the number of partitions defined for the topic.

* *$topic* **delete**

 Delete the producer object, destroying the command.

Methods of kafka topic consumer object
---

* *$topic* **consume_start** *partition* *offset* *?callback?*

 Start consuming the established topic for the specified *partition* starting at offset *offset*.

 *offset* can be **beginning** to start consuming at the beginning of the partition (i.e. the oldest message in the partition), **end** to start consuming from the end of the partition, **stored** to start consuming from the offset retrieved from the offset store, a positive integer, which starts consuming starting from the specified message number from that partition, or a negative integer, which says to start consuming that many messages from the end.

 If callback is specified then the callback will be invoked with an argument containing a list of key-value pairs representing an error or a successfully received message.

 If a message is successfully produced the list will contain several key-value pairs:

  * payload - the message previously queued to kafka

  * partition - the partition number the payload was received from

  * offset - the kafka offset (basically the kafka message ID) corresponding to this message

  * topic - the name of the topic the message was received from, if known.

  * key  - the partitioner key that was specified, if one was specified.

 If an error is encountered the message will contain:

  * error - the kafka error string returned by *rd_kafka_err2str*

  * code - the kafka error code string, such as **RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT**.

  * message - the error message from the server

* *$topic* **consume_start_queue** *partition* *offset* *queue*

 Start consuming the established topic for the specified *partition*, starting at offset *offset*, re-routing incoming messages to the specified kafkatcl *queue* command object.

 Multiple topics and partitions can be routed to the same queue.

 The queue should have been created with the *$handle* **create_queue** *command* method described elsewhere in this doc.

* *$topic* **consume_stop** *partition*

 Stop consuming messages for the established topic and specified *partition*, purging all messages currently in the local queue.

* *$topic* **consume** *partition* *timeout* *array*

 Consume one message from the topic object for the specified partition received within *timeout* milliseconds into array *array*.  If there's an error, you get a Tcl error.

 Returns 1 on success and 0 if you reach the end of the partition.  You can read more subsequently.

 On success array will be populated with the following fields:

  * payload - the message previously queued to kafka

  * partition - the partition number the payload was received from

  * offset - the kafka offset (basically the kafka message ID) corresponding to this message

  * topic - the name of the topic the message was received from

  * key  - the partitioner key that was specified, if one was specified.

* *$topic* **consume_batch** *partition* *timeout* *count* *array* *code*

 Consume up to *count* messages or however many have come in less than that within *timeout* milliseconds.

 For each message received fill the array *array* with fields from the message containing the message *payload*, *partition*, *offset*, *topic* name and optional *key*, repeatedly executing *code* for each message received.

 This method returns number of rows processed, 0 if the end of the partition is reached.

* *$topic* **info** **topic**

Return the name of the topic.

* *$topic* **info** **partitions**

Return the number of partitions defined for the topic.

* *$topic* **info** **consistent_partition** *key*

Return the partition number that the Kafka consistent partitioner will return for the given key for the number of partitions defined for the topic.

* *$topic* **delete**

 Delete the topic, destorying the corresponding Tcl command.

Methods of kafka topic consumer queue object
---
Queue objects are created using *$handle* **create_queue** *command* and messages from consumer topics can be fed to a queue using the **consume_start_queue** method of a topic consumer object.

Queue objects support the following methods:

* *$queue* **consume** *timeoutMS* *array*

 As with consuming from a topic consumer, the **consume** method of a queue consuimes one message from the corresponding queue, within *timeout* milliseconds, into array *array*.  If there's an error, you get a Tcl error.

 Unlike with the **consume** method of a topic consumer, the partition is not specified at this point, as it has been specified when **consume_start_queue** was used to hook a consumer into a local queue.

* *$queue* **consume_batch** *timeoutMS* *count* *array* *code*

 As with consuming a batch from a topic, reads up to *count* messages or however many have come in less than that within *timeout* milliseconds.

 For each message received the array *array* is filled with fields from the message containing the message *payload*, *partition*, *offset*, *topic* name and optional *key*, repeatedly executing *code* for each message received.

 The method returns number of rows processed.

* *$queue* **consume_callback** *?callback?*

 If callback is specified, sets things so that the callback routine will be invoked for each message present in the queue.

 If callback is an empty string then it turns off the callback function.

 If no callback argument is specified, the current callback is returned; an empty string is returned if no callback is currently defined.

*$queue* **delete**

 Delete the consumer queue object.

Received Kafka Messages
---

You specify the name of the array and if there isn't an error the array is populated with the following info from the message:

* payload

 The message payload.

* partition

 The partition number.

* key

 The key will be provided if the (optional) key was specified when the message was produced.

* offset

 The offset.

* topic

 The name of the topic.

Setting up a consumer
---

Create a kafka object, from that use the *consumer_creator* method to create a kafka consumer-creating object.  From that create a topic-consuming object to read messages.

```tcl
package require kafka

::kafka::kafka create kafka_master

kafka_master consumer_creator kafka_consumer
kafka_consumer add_brokers 127.0.0.1

kafka_consumer new_topic consumer test

consumer consume_start 0 0

consumer consume 0 2000 foo
parray foo
```

Setting up a consumer with callbacks
---

Create a kafka object, from that use the *consumer_creator* method to create a kafka consumer-creating object.  From that create a topic consuming object to read messages and invoke it in the callback manner.

```
package require kafka

set master [::kafka::kafka create #auto]

set consumer [$master consumer_creator #auto]
$consumer add_brokers 127.0.0.1

set topic [$consumer new_topic #auto flightplans.production]

proc callback {list} {
	puts $list
}

$topic consume_start 0 beginning callback
```

Setting up a consumer and consume to a queue with callbacks
---

Create a kafka object, from that use the *consumer_creator* method to create a kafka consumer-creating object.  From that create a topic consuming object to read messages and invoke it in the callback manner.

```
package require kafka

set master [::kafka::kafka create #auto]

set consumer [$master consumer_creator #auto]
$master add_brokers 127.0.0.1

set myQueue [$consumer create_queue #auto]

set topic [$consumer new_topic #auto flightplans.production]

proc callback {list} {
	puts $list
}

$topic consume_start_queue 0 beginning $myQueue

```


Setting up a producer
---

Create a kafka object, from that use the *producer_creator* method to create a kafka producer-creating object.  From that create a topic producing object to produce messages to.  key is optional.

```tcl
package require kafka

::kafka::kafka create kafka_master

kafka_master producer_creator kafka_producer
kafka_producer add_brokers 127.0.0.1


kafka_producer new_topic producer test

producer produce $partition $payload $key
```

KafkaTcl The Easy Way
---

Kafkatcl has the ability to have multiple master objects and multiple topic-consumer-generating and topic-producer-generating commands.  It's all very thorough.  But unless you're planning to talk to multiple kafka clusters from one process you may be happier with the easy interface provided by the kafkatcl library.

* ::kafka::setup

 Setup easy kafka by creating a kafka master object named kafka::master.  Can safely be called multiple times.  setup is invoked implicitly by other functions but may need to be invoked explicitly if you want to setup callbacks and whatnot before defining a producer.

* **kafka::brokers** *brokerList*

 Assign a list of brokers.  Default is 127.0.0.1.

* **kafka::topic_consumer** *command* *topic*

 Create a kafkatcl topic-consuming command named *command* attached to *topic*.

* **kafka::topic_producer** *command* *topic*

 Create a kafkatcl topic-producing command named *command* attached to *topic*.

 It create on demand one master object, ::kafka::master, one producer object, ::kafka::producer, one consumer object, ::kafka::consumer.

 You can configure them and do all the stuff with them.

* **kafka::setup_producer**

 You don't need this unless you want to configure the producer object before creating a topic producer.  Likewise for setup_consumer.

 Once setup_producer has been invoked to create a producer-generating object called kafka::producer, changes to the master object like defining callbacks will not be inherited by the producer or consumer creator.


Misc Stuff
---

Sample output of **kafka_master config**:

```
client.id rdkafka message.max.bytes 4000000 receive.message.max.bytes 100000000 metadata.request.timeout.ms 60000 topic.metadata.refresh.interval.ms 10000 topic.metadata.refresh.fast.cnt 10 topic.metadata.refresh.fast.interval.ms 250 topic.metadata.refresh.sparse false socket.timeout.ms 60000 socket.send.buffer.bytes 0 socket.receive.buffer.bytes 0 socket.keepalive.enable false socket.max.fails 3 broker.address.ttl 300000 broker.address.family any statistics.interval.ms 0 log_cb 0x802e0e850 log_level 6 socket_cb 0x802e13560 open_cb 0x802e21fd0 opaque 0x801ec6910 internal.termination.signal 0 queued.min.messages 100000 queued.max.messages.kbytes 1000000 fetch.wait.max.ms 100 fetch.message.max.bytes 1048576 fetch.min.bytes 1 fetch.error.backoff.ms 500 queue.buffering.max.messages 100000 queue.buffering.max.ms 1000 message.send.max.retries 2 retry.backoff.ms 100 compression.codec none batch.num.messages 1000 delivery.report.only.error false
```

Sample output of **kafka_master topic_config**:
```
request.required.acks 1 enforce.isr.cnt 0 request.timeout.ms 5000 message.timeout.ms 300000 produce.offset.report false opaque 0x801ec6910 auto.commit.enable true auto.commit.interval.ms 60000 auto.offset.reset largest offset.store.path . offset.store.sync.interval.ms -1 offset.store.method file consume.callback.max.messages 0
```
