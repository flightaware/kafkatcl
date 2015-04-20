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

Batches
---

Batching CQL can yield a huge performance increase.

While you can construct batches out of straight CQL with BEGIN BATCH and APPLY BATCH, casstcl provides batch objects to help you construct, manage and use batches.

Create a new batch using the batch method of the cassandra object:

```tcl
$cassdb create mybatch
set mybatch [$cassdb create #auto]
```

Both styles work.

* *$batch* **add** *?-table tableName?* *?-array arrayName?* *?-prepared preparedObjectName? ?args..?

Adds the specified statement to the batch. Processes arguments similarly to the **exec** and **async** methods.

* *$batch* **consistency** *?$consistencyLevel?*

Sets the batch write consistency level to the specified level.  Consistency must be **any**, **one**, **two**, **three**, **quorum**, **all**, **local_quorum**, **each_quorum**, **serial**, **local_serial**, or **local_one**.

* *$batch* **upsert** ?-mapunknown mapcolumn? ?-nocomplain? ?-ifnotexists? *$table* *$list*

Generate in upsert into the batch.  List contains key value pairs where keys should be columns that exist in the fully qualified table.

A typical usage would be:

```tcl
$batch upsert wx.wx_metar [array get row]
```

If *-nocomplain* is specified then any column names not found in the column map for the specified table are silently dropped along with their values.

If *-mapunknown* is specified then an additional argument containing a column name should be specified.  With this usage any column names not found in the column map are written to a map collection of the specified column name.  The column name must exist as a column in the table and be of the *map* type and the key and values types of the map must be *text*.

* *$batch* **count**

Return the number of rows added to the batch using *add* or *upsert*.

* *$batch* **reset**

Reset the batch by deleting all of its data.

* *$batch* **delete**

Delete the batch object and all of its data.

As of Cassandra 3.1 there is a limit on the maximum size of a batch in the vicinity of 16 megabytes.  Generating batches that are too large will result in "no hosts available" when trying to send the batch and illegal argument exceptions on the backend from java about a mutation being too large.

Configuring SSL Connections
---

* *$cassdb* **ssl_cert** *$pemFormattedCertString*

This sets the client-side certificate chain.  This is used by the server to authenticate the client.  This should contain the entire certificate chain starting with the certificate itself.

* *$cassdb* **ssl_private_key** *$pemFormattedCertString $password*

This sets the client-side private key.  This is by the server to authenticate the client.

* *$cassdb* **add_trusted_cert** *$pemFormattedCertString*

This adds a trusted certificate.  This is used to verify the peer's certificate.

* *$cassdb* **ssl_verify_flag** *$flag*

This sets the verification that the client will perform on the peer's certificate.  **none** selects that no verification will be performed, while **verify_peer_certificate** will verify that a certificate is present and valid.

Finally, **verify_peer_identity** will match the IP address to the certificate's common name or one of its subject alternative names.  This implies that the certificate is also present.

Future Objects
---

Future objects are created when casstcl's async method is invoked.  It is up to the user to ensure that the objects returned by the async method have their own methods invoked to enquire as to the status and ultimate disposition of their requests.  After you're finished with a future object, you should delete it.

```tcl
set future [$cassObj async "select * from wx_metar where airport = 'KHOU' order by time desc limit 1"]

# see if the request was successful
if {[$future isready]} {
	if {[$future status] != "CASS_OK"} {
		set errorString [$future error_message]
	}
}
```

* *$future* **isready**

Returns 1 if the future (query result) is ready, else 0.

* *$future* **wait** *?us?*

Waits for the request to complete.  If the optional argument us is specified, times out after that number of microseconds.

* *$future* **foreach** *rowArray code*

Iterate through the query results, filling the named array with the columns of the row and their values and executing code thereupon.

* *$future* **status**

Return the cassandra status code converted back to a string, like CASS_OK and CASS_ERROR_SSL_NO_PEER_CERT and whatnot.  If it's anything other than CASS_OK then whatever you did didn't work.

* *$future* **error_message**

Return the cassandra error message for the future, empty string if none.

* *$future* **delete**

Delete the future.  Care should be taken to delete these when done with them to avoid leaking memory.

Casstcl logging callback
---

The Cassandra cpp-driver supports custom logging callbacks to allow an application to receive logging messages rather than having them go to stderr, which is what happens by default, and casstcl provides Tcl-level support for this capability.

The logging callback is defined like this:

```tcl
::casstcl::cass logging_callback callbackFunction
```

When a log message callback occurs from the Cassandra cpp-driver, casstcl will obtain that and invoke the specified callback function one argument containing a list of key value pairs representing the (currently six) things that are received in a logging object:

* "clock" and a floating point epoch clock with millisecond accuracy

* "severity" and the cassandra log level as a string.  value will be one of **disabled**, **critical**, **error**, **warn**, **info**, **debug**, **trace** or **unknown**.

* "file" and possibly the name of some file, or an empty string... not sure what it is, maybe the source file of the cpp-driver that is throwing the error.

* "line" and some line number, not sure what it is, probably the line number of the source file of the cpp-driver that is throwing the error

* "function" and the name of some function, probably the cpp-driver function that is throwing the error

* "message" and the error message itself

The level of detail queued to the logging callback may be adjusted via:

```tcl
::casstcl::cass log_level level
```

This sets the log level for the cpp-driver.  The legal values, in order from least output (none) to most output (all) are "disabled", "critical", "error", "warn", "info", "debug", and "trace".

The default is "warn".

A sample implementation:

```tcl
proc logging_callback {pairList} {
    puts "logging_callback called..."
    array set log $pairList
    parray log
    puts ""
}

casstcl::cass logging_callback logging_callback
casstcl::cass log_level info
```

According to the cpp-driver documentation, logging configuration should be done before calling any other driver function, so if you're going to use this, invoke it after package requiring casstcl and before invoking ::casstcl::cass create.

Also note that logging is global.  That is, it's not tied to a specific connection if you had more than one connection for some reason.  Also currently it is not fully cleaned up if you unload the package.

It is important to at least capture this information as useful debuggin information sometimes comes back through this interface.  For example you might just get a "Query error" back from the library but then you get a logging callback that provides much more information, such as

```
:371:int32_t cass::decode_buffer_size(const cass::Buffer &)): Routing key cannot contain an empty value or a collection
```

The above messages makes the problem much more clear.  The caller probably didn't specify a value for the primary key.

Casstcl library functions
---

The library functions such as *download_schema*, *list_schema*, *list_tables*, etc, have been removed.  These capabilities are now provided by the metadata-traversing methods *list_keyspaces*, *list_tables*, *list_columns* and *list_column_types* documented above.


* **::casstcl::validator_to_type** *$type*

Given a validator type such as "org.apache.cassandra.db.marshal.AsciiType" and return the corresponding Cassandra type.  Can decode complex type references including reversed, list, set, and map.  This function is used by casstcl's *list_column_types* method.

* **::casstcl::quote** *$value* *$type*

Return a quote of the specified value according to the specified cassandra type.

* **casstcl::typeof** *table.columnName*

* **casstcl::quote_typeof** *table.columnName* *value* *?subType?*

Quote the value according to the field *columnName* in table *table*.  *subType* can be **key** for collection sets and lists and can be **key** or **value** for collection maps.  For these usages it returns the corresponding data type.  If the subType is specified and the column is a collection you get a list back like *list text* and *map int text*.

* **casstcl::assemble_statement** *statementVar* *line*

Given the name of a variable (initially set to an empty string) to contain a CQL statement, and a line containing possibly a statement or part of a statement, append the line to the statement and return 1 if a complete statement is present, else 0.

Comments and blank lines are skipped.

(The check for a complete statement is the mere presence of a semicolon anywhere on the line, which is kind of meatball.)

* **run_fp** *cassdb* *fp*

Read from a file pointer and execute complete commands through the specified cassandra object.

* **run_file** *cassdb* *fileName*

Run CQL commands from a file.

* **interact** *cassdb*

Enter a primitive cqlsh-like shell.  Provides a prompt of *tcqlsh>* when no partial command has been entered or *......>* when a partial command is present.  Once a semicolon is seen via one input line or multiple, the command is executed.  "exit" to exit the interact proc.

errorCode
---

When propagating an error from the cpp-driver back to Tcl, the errorCode will be a list containing three our four elements.  The first will be the literal string **CASSANDRA**, the second will be the cassandra error code in text form matching the error codes in the cassandra.h include file, for example **CASS_ERROR_LIB_NO_HOSTS_AVAILBLE** or **CASS_ERROR_SSL_INVALID_CERT**.

The third value will be the output of the cpp-drivers *cass_error_desc()* call, returning a result such as "Invalid query".

The fourth value, if present, will be the output of *cass_future_error_message()*.  When present it is usually pretty specific as to the nature of the problem and quite useful to aid in debugging.

Both error messages are used to generate the error string itself, to the degree that they are available when the tcl error return is being generated.

Bugs
---

Some Cassandra data types are unimplemented or broken.  Specifically *unknown*, *custom*, *decimal*, *varint*, and *timeuuid* either don't or probably don't work.

Memory leaks are a distinct possibility.

The code is new and therefore hasn't gotten a lot of use.  Nonetheless, the workhorse functions **async**, **exec** and **select** seem to work pretty well.
