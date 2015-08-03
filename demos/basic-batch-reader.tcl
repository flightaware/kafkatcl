#
# consuming batches of messages from kafka
#

package require kafka

::kafka::kafka create kafka_master

kafka_master consumer_creator kafka_consumer
kafka_consumer add_brokers 127.0.0.1

kafka_consumer new_topic consumer my.topic

consumer consume_start 0 -10

proc pass {} {
	while true {
		# consume up to 1000 items from partition 0 with a 2 second timeout
		puts [consumer consume_batch 0 2000 1000 row {
			parray row
			puts ""
		}]
	}
}

pass

