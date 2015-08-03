#
# kafka producer demo
#
#
package require kafka

proc klog {list} {
	puts "klog: $list"
}

proc delivered {list} {
	puts "delivered: $list"
}

# create a kafka master object
::kafka::kafka create kafka_master

# arrange for logging callbacks to call klog and set the desired log level
kafka_master logger callback klog
kafka_producer log_level debug

# create a producer creator and add a broker or two
kafka_master create_producer kafka_producer
kafka_producer add_brokers 127.0.0.1

# arrange for a delivery report callback
kafka_master delivery_report_callback delivered

# enable offset reporting
puts [kafka_producer config produce.offset.report true]

# get the config and dump it
array set producerConfig [kafka_producer config]
parray producerConfig

# say what topic we are producing to
kafka_producer new_topic producer my.test

# for each line read from stdin produce to partition 0
while {[gets stdin line] >= 0} {
	producer produce 0 $line
}

