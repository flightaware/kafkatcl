#
# consume from many partitions into a queue with callbacks
#

package require kafka

kafka::brokers 127.0.0.1

kafka::setup_consumer

kafka::consumer create_queue myQueue

set topic my.test

kafka::consumer new_topic consumer $topic

array set consumerConfig [kafka::consumer config]
parray consumerConfig

proc callback {list} {
	array set row $list
	puts [array names row]
	#puts $list
}

myQueue consume_callback callback

# start consuming from many partitions into myQueue
for {set i 0} {$i < 8} {incr i} {
	consumer consume_start_queue $i beginning myQueue
}

vwait die
