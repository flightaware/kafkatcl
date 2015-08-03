#
# producer with delivery report callback
#

package require kafka

proc dr_callback {args} {
	puts "dr_callback: $args"
}

kafka::brokers 127.0.0.1

# ask for a delivery report callback every 10 messages produced
kafka::master delivery_report callback dr_callback
kafka::master delivery_report every 10

#kafka::master config compression.codec snappy

kafka::setup_producer

kafka::producer config produce.offset.report true

kafka::topic_producer producer my.test

while {[gets stdin line] >= 0} {
	producer produce 0 $line
}

# vim: set ts=4 sw=4 sts=4 noet :

