#
# kafkatcl support functions
#
#
#

namespace eval ::kafka {
	variable consumerIsSetup 0
	variable producerIsSetup 0
	variable masterIsSetup 0
	variable brokers 127.0.0.1

#
# setup - create a kafka master object if one hasn't already been created
#
proc setup {} {
	variable masterIsSetup

	if {$masterIsSetup} {
		return
	}

	::kafka::kafka create ::kafka::master
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
}

#
# brokers - specify a list of brokers
#
proc brokers {brokerList} {
	variable brokers

	set brokers $brokerList
}

#
# topic_producer - given a name and a topic create a kafka command that can
#   consume from the topic
#
proc topic_producer {name topic} {
	setup_producer

	return [producer new_topic $name $topic]
}

#
# topic_consumer - given a name and a topic create a kafka command that can 
#   produce to the topic
#
proc topic_consumer {name topic} {
	setup_consumer

	return [consumer new_topic $name $topic]
}


} ;# namespace ::kafka

# vim: set ts=4 sw=4 sts=4 noet :

