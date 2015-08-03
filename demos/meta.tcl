#
# simple demo to get some of the metadata
#

package require kafka

proc klog {list} {
	puts "klog: $list"
}

::kafka::kafka create kafka_master

kafka_master logger callback klog

kafka_master consumer_creator kafka_consumer
kafka_consumer add_brokers 127.0.0.1

puts "brokers: [lsort [kafka_consumer info brokers]]"

puts "topics: [lsort [kafka_consumer info topics]]"

foreach topic [lsort [kafka_consumer info topics]] {
	puts "  topic $topic partitions: [kafka_consumer info partitions $topic]"
}

