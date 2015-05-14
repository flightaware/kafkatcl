KafkaTcl, a Tcl C extension providing a Tcl interface to the Apache Kafka distributed messaging system
===

CassTcl provides a Tcl interface to the KafkaTcl C language API.

Functionality
---

- Can be used completely asynchronous
- Can be used synchronously for convenience
- Provides a natural Tcl interface
- Thread safe
- Free!

License
---

Open source under the permissive Berkeley copyright, see file LICENSE

Requirements
---
Requires the Apache Kfka library librdkafka be installed.  (https://github.com/edenhill/librdkafka)

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

* *$kafka* **delivery_report_callback** *command*

Invoke *command* when kafka cpp-driver delivery report callbacks are received.

Data returned is the same for *consume_callback*.

* *$kafka* **error_callback** *command*

Invoke *command* when kafka cpp-driver error callbacks are received.

* *$kafka* **statistics_callback** *command*

Invoke *command* when kafka cpp-driver statistics callbacks are received.

The command will be invoked with one argument, which is the JSON provided by the stats callback.

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

Return the kafka name of the handle.

* *$handle* **config** *?key value?*

The kafka handle object inherits a copy of the topic config from the master object.

**config** provides a way to examine and alter the handle's topic configuration properties.  (These properties are accessed when the producer/consumer objects are created.)

If invoked without arguments returns a list of the topic configuration properties and values.

If invoked with one or more pairs of property and value arguments, sets the value of each property-value pair into the topic configuration properties.

When a topic-producing or topic-consuming object is created, the topic config is set from the corresponding handle object that created the producer or consumer object.

```
    $handle config compression.codec gzip
```

* *$handle* **poll** *?timeoutMS?*

Polls the kafka handle for events.  Events will cause kafkatcl-provided kafka event callbacks to be called.

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

* *$topic* **delete**

Delete the producer object, destroying the command.

Methods of kafka topic consumer object
---

* *$topic* **consume_start** *partition* *offset*

Start consuming the established topic for the specified *partition* starting at offset *offset*.

*offset* can be **beginning** to start consuming at the beginning of the partition (i.e. the oldest message in the partition), **end** to start consuming from the end of the partition, **stored** to start consuming from the offset retrieved from the offset store (whatever that means), a positive integer, which starts consuming starting from the specified message number from that partition, or a negative integer, which says to start consuming that many messages from the end.

* *$topic* **consume_start_queue** *partition* *offset* *queue*

Start consuming the established topic for the specified *partition*, starting at offset *offset*, re-routing incoming messages to the specified kafkatcl *queue* command object.

The queue should have been created with the *$handle* **create_queue** *command* method described elsewhere in this doc.

* *$topic* **consume_stop** *partition*

Stop consuming messages for the established topic and specified *partition*, purging all messages currently in the local queue.

* *$topic* **consume** *partition* *timeout* *array*

Consume one message from the topic object for the specified partition received within *timeout* milliseconds into array *array*.  If there's an error, you get a Tcl error.

* *$topic* **consume_batch** *partition* *timeout* *count* *array* *code*

Consume up to *count* messages or however many have come in less than that within *timeout* milliseconds.

For each message received fill the array *array* with fields from the message containing the message *payload*, *partition*, *offset*, *topic* name and optional *key*, repeatedly executing *code* for each message received.

This method returns number of rows processed.

* *$topic* **consume_callback** *partition* *timeoutMS* *command*

Repeated invoke *command* with one argument containing a list of key-value pairs for a received message, or a list of key-value pairs reporting an error.

If a message was successfully received the fields will be *payload*, *partition*, *offset*, *topic* and, optionally, *key*.

If an error is received the fields will be *error*, *code*, and *message* corresponding to the kafka error string returned by rd_kafka_err2str, the kafka error code and an error message.

**consume_callback** returns the number of messages consumed.

Note that you have to call *consume_callback* repeatedly and it may return 0 if no data is available at the time it is called.  Subsequent calls will return nonzero when messages are available.

* *$topic* **delte**

Delete the topic, destorying the corresponding Tcl command.

```
consumer consume_callback 0 5000 callback
```

Consume messages from partition 0 by invoking the routine **callback** and timeout after a maximum of 5000 ms.

* *$topic* **delete**

Delete the topic object.

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

* *$queue* **consume_callback** *timeout* *command*

As with using a callback to consume from a topic, repeatedly invokes *command* with one argument containing a list of key-value pairs for a received message, or a list of key-value pairs reporting an error.

If the message was successfully received then the pairs will be *payload*, *partition*, *offset*, *topic* and, optionally, *key*.

If an error is received the fields will be *error*, *code*, and *message* corresponding to the kafka error string returned by rd_kafka_err2str, the kafka error code and an error message.

**consume_callback** returns the number of messages consumed.

Note that you have to invoke the *consume_callback* method repeatedly, and it will return 0 if no data is available at the time it is called.  Later, though, calling it again may well produce messages.

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

$topic consume_start 0 beginning

proc callback {list} {
	puts $list
}

set rowsReceived [$topic consume_callback 0 5000 callback]
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

$topic consume_start_queue 0 beginning $myQueue

proc callback {list} {
	puts $list
}

proc pass {} {
	puts [$::myQueue consume_callback 5000 callback]
}

pass

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
