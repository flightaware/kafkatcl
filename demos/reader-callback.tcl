#
# demo of kafka readers using callbacks
#

package require kafka

::kafka::kafka create kafka_master

kafka_master consumer_creator kafka_consumer
kafka_consumer add_brokers 127.0.0.1

kafka_consumer new_topic consumer my.topic

proc callback {list} {
	puts $list
}

# consume partition 0 starting from the beginning, calling callback as
# payloads become available
consumer start 0 beginning callback

vwait die

