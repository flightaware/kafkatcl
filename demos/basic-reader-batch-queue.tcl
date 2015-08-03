#
# basic batch reader via a queue
#

package require kafka

::kafka::kafka create kafka_master

kafka_master consumer_creator kafka_consumer
kafka_consumer add_brokers 127.0.0.1

kafka_consumer new_topic consumer my.topic

kafka_consumer create_queue myQueue

# tell kafka to start consuming into a queue.
#
# you can consume from multiple partitions into the same queue although
# you can't control their ordering.  Also from multiple topics, etc.
#
consumer consume_start_queue 0 beginning myQueue

proc pass {} {
	set count [myQueue consume_batch 2000 100 row {
		parray row
		puts ""
	}]
	return $count
}

pass

