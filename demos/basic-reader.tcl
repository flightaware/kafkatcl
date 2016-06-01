
package require kafka

::kafka::kafka create kafka_master

kafka_master consumer_creator kafka_consumer
kafka_consumer add_brokers 127.0.0.1

set topic my.topic

kafka_consumer new_topic consumer $topic

# start reading partition 0 from its beginning
#consumer start 0 beginning

# start reading partition 0 a few positions from the end
consumer start 0 -10

#
# consume continuously from partition 0 of my.topic 
#
# timeout of -1 says to wait forever for new messages to appear
#
while {[consumer consume 0 -1 row]} {
	parray row
	puts ""
}
