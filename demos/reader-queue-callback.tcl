#
# read from a topic into a queue with callbacks
#

package require kafka

::kafka::kafka create kafka_master

kafka_master consumer_creator kafka_consumer

array set config [kafka_master config]
parray config

kafka_consumer add_brokers 127.0.0.1

kafka_consumer create_queue myQueue

array set consumerConfig [kafka_consumer config]
parray consumerConfig

kafka_consumer new_topic consumer my.topic

consumer consume_start_queue 0 beginning myQueue

proc callback {list} {
	puts $list
}

myQueue consume_callback callback

vwait die
