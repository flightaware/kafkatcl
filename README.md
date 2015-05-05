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


KafkaTcl command
---

The kafkatcl command from which all others are created is **::kafkatcl::kafka**.

* **kafkatcl::kafka** **version**

Returns the kafka version as a string, like 0.8.6.

* **kafkatcl::kafka** **create** *command*

Create a kafka object.

```tcl
set kafka [::kafkatcl::kafka create #auto]

$kafka exec "insert into foo ..."
```

...or...

```tcl
::kafkatcl::kafka create kafka]

kafka exec "insert into foo ..."
```

Methods of kafka interface object
---

* *$kafka* **config**

Set a configuration property by specifying the property name and value.  Returns a Tcl error if it fails.

* *$kafka* **create_producer** *cmdName*

Create a kafkatcl producer handle object.

* *$kafka* **create_consumer** *cmdName*

Create a kafkatcl consumer handle object.

* *$kafka* **set_topic_conf** *key* *value*

Set a single kafka topic configuration property  by specifying the property name and value.  Returns a Tcl error if it fails.

* *$kafka* **set_delivery_report_callback** 

Not yet implemented.

* *$kafka* **set_error_callback** 

Not yet implemented.

* *$kafka* **set_logger_callback** 

Not yet implemented.

* *$kafka* **set_statistics_callback** 

Not yet implemented.

* *$kafka* **set_socket_callback** 

Not yet implemented.

* *$kafka* **get_configuration** *array*

Store the kafka object's configuration properties into the specified array.

* *$kafka* **get_topic_configuration**  *array*

Store the kafka object's topic configuration properties into the specified array.

* *$kafka* **logging_callback** 

Methods of kafka handle object
---

* *$handle* **name**

Return the kafka name of the handle.

* *$handle* **new_topic** *commandName* *topic*

Creates a new topic consumer or producer object corresponding to the specified topic.

If the handle was created with the **create_producer** method then a topic producer object will be created.  If it was created with the **create_consumer** method then a topic consumer object will be created.

* *$handle* **log_level** *level*

Set the log level to one of **emerg**, **alert**, **crit**, **err**, **warning**, **notice**, **info**, or **debug**.

* $handle* **add_brokers** *brokerList*

Add a list of kafka brokers to the kafka handle object.

* $handle* delete

Delete the handle object, destroying the command.

Methods of kafka topic producer object
---

* *$topic* **produce_one** *partition* *payload* *?key?*

Produce one message into the specified partition.  If there's an error, you get a Tcl error.  IF the partition is -1 then the unassigned partition is specified, indicating that kafka should partition using the configured or default partitioner.

If *key* is specified then it's passed to the topic partitioner as well as sent to the broker and passed to the consumer.  That means the partitioning algorithm can use that to help pick the partition.  Also it's a value that can be sent through alongside the payload.

Methods of kafka topic consumer object
---

* *$topic* **consume_one** *partition* *timeout* *array*

Consume one message from the specified partition without *timeout* milliseconds into array *array*.  If there's an error, you get a Tcl error.

* *$topic* **consume_batch** *partition* *timeout* *count* *array* *code*

Read up to *count* messages or however many have come in less than that without *timeout* milliseconds.

For each message received fill the array *array* with fields from the message containing the message *payload*, *partition*, *offset*, *topic* name and optional *key*, repeatedly executing *code* for each message received.

*offset* can be **beginning** to start consuming at the beginning of the partition (i.e. the oldest message in the partition), **end** to start consuming from the end of the partition, **stored** to start consuming from the offset retrieved from the offset store, whatever that means, a positive integer, which starts consuming starting from the specified message number from that partition, or a negative integer, which says to start consuming that many messages from the end.

* *$topic* **consume_start** *partition* *offset*

Start consuming the established topic for the specified *partition* starting at offset *offset*.

* *$topic* **consume_start_queue** *partition* *offset* *queue*

Start consuming the established topic for the specified *partition*, starting at offset *offset*, re-routing incoming messages to the specified kafkatcl *queue* command object.

* *$topic* **consume_stop** *partition*

Stop consuming messages for the established topic and specified *partition*, purging all messages currently in the local queue.

* *$topic* **consume_callback**

Not implemented yet.

* $topic* **delete**

Delete the topic object.

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

Create a kafka object, from that use the *create_consumer* method to create a kafka consumer-creating object.  From that create a topic consuming object to read messages.

```tcl
package require kafka

::kafkatcl::kafka create kafka_master

kafka_master create_consumer kafka_consumer

kafka_consumer new_topic consumer test

consumer consume_start 0 0

consumer consume_one 0 2000 foo
parray foo
```

Setting up a producer
---

Create a kafka object, from that use the *create_producer* method to create a kafka producer-creating object.  From that create a topic producing object to produce messages to.  key is optional.

```tcl
package require kafka

::kafkatcl::kafka create kafka_master

kafka_master create_producer kafka_producer

kafka_producer new_topic producer test

producer produce_one $partition $payload $key
```
