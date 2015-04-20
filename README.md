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

You've got handle objects, from handles you create topic objects.

Topic objects are used to consume or produce messages to kafka.


KafkaTcl objects
---

KafkaTcl provides object creation commands...

::kafkatcl::kafka create handle, to create a kafka object.

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

* *$kafka* **add_brokers** *brokerList*

* *$kafka* **create_producer** *cmdName*

Create a producer handle object.

* *$kafka* **create_consumer** *cmdName*

Create a consumer handle object.

* *$kafka* **set_topic_conf** 

* *$kafka* **set_delivery_report_callback** 

* *$kafka* **set_error_callback** 

* *$kafka* **set_logger_callback** 

* *$kafka* **set_statistics_callback** 

* *$kafka* **set_socket_callback** 

* *$kafka* **get_configuration** 

* *$kafka* **get_topic_configuration** 

* *$kafka* **logging_callback** 

* *$kafka* **log_level** 

Methods of kafka handle object
---

* *$handle* **name**

Return the kafka name of the handle.

* *$handle* **new_topic** *commandName* *topic*

Creates a new topic command representing the specified topicrepresenting the specified topic

* $handle* delete

Delete the handle.

Methods of kafka topic object
---

* *$topic* **consume_one** *partition* *timeout* *array*

Consume one message from the specified partition without *timeout* milliseconds into array *array*.  If there's an error, you get a Tcl error.

Messages contain

* *$topic* **consume_one**

* *$topic* **consume_batch**

* *$topic* **consume_start**

* *$topic* **consume_start_queue**

* *$topic* **consume_stop**

* *$topic* **consume_callback**

* *$topic* **partition_available**

* $topic* **delete**

Delete the topic.

Received Kafka Messages
---

You specify the name of the array and if there isn't an error the array is populated with the following info from the message:

* payload

The message payload.

* partition

The partition number.

* key

The optional key.

* offset

The offset.

* topic

The name of the topic.

