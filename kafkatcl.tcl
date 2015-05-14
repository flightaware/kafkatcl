#
# kafkatcl support functions
#
# simple usage:
#
# kafka::brokers $brokerList
# topic_producer commandName topic
# topic_consumer commandName topic
#

namespace eval ::kafka  {
	variable consumerIsSetup 0
	variable producerIsSetup 0
	variable masterIsSetup 0
	variable brokers 127.0.0.1

proc logger {message} {
	puts stderr "kafka: $message"
}
#
# setup - create a kafka master object if one hasn't already been created
#
proc setup {} {
	variable masterIsSetup

	if {$masterIsSetup} {
		return
	}

	::kafka::kafka create ::kafka::master
	logger "created ::kafka::master"
	set masterIsSetup 1
}

#
# setup_consumer - create a kafka consumer object if one hasn't already
# been created.  perform basic setup if necessary.
#
proc setup_consumer {} {
	variable consumerIsSetup
	variable brokers

	if {$consumerIsSetup} {
		return
	}

	setup

	master consumer_creator ::kafka::consumer
	consumer add_brokers $brokers

	set consumerIsSetup 1

	logger "created consumer-creator with brokers $brokers"
}

#
# setup_producer - create a kafka producer object if one hasn't already
# been created.  perform basic setup if necessary.
#
proc setup_producer {} {
	variable producerIsSetup
	variable brokers

	if {$producerIsSetup} {
		return
	}

	setup

	master producer_creator ::kafka::producer
	producer add_brokers $brokers

	producer config compression.codec gzip

	set producerIsSetup 1

	logger "created producer-creator with brokers $brokers"
}

#
# brokers - specify a list of brokers
#
proc brokers {brokerList} {
	variable brokers

	set brokers $brokerList

	setup

	logger "set brokers to $brokerList"
}

#
# topic_producer - given a name and a topic create a kafka command that can
#   consume from the topic
#
proc topic_producer {name topic} {
	setup_producer

	logger "creating producer $name for topic $topic"
	return [producer new_topic $name $topic]
}

#
# topic_consumer - given a name and a topic create a kafka command that can 
#   produce to the topic
#
proc topic_consumer {name topic} {
	setup_consumer

	logger "creating consumer $name for topic $topic"
	return [consumer new_topic $name $topic]
}


} ;# namespace ::kafka

# vim: set ts=4 sw=4 sts=4 noet :

