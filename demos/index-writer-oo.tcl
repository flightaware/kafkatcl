#
# itcl class to write to a topic and write periodically to an index topic
#
#

package require kafka
package require Itcl

::itcl::class KafkaIndexWriter {
	public variable topic ""
	public variable topicIndex
	public variable producer
	public variable master
	public variable brokers 127.0.0.1
	public variable topicObj
	public variable indexTopicObj
	public variable partition 0
	public variable sampleFrequencyMS 300000

	constructor {args} {
		configure {*}$args

		if {$topic == ""} {
			error "must set -topic when creating"
		}

		# create kafka master handle
		set master [kafka::kafka create #auto]

		# define a delivery callback and turn off
		# invoking tcl for each callback
		$master delivery_report callback [list $this dr_callback]
		$master delivery_report every 0

		# create a producer object and connect
		set producer [$master producer_creator #auto]
		$producer add_brokers $brokers

		# enable offset reports, this makes the callbacks
		# happen
		$producer config produce.offset.report true

		# create the topic
		set topicObj [$producer new_topic #auto $topic]

		# ok we don't want offset reports on the next topic,
		# which is the index topic we are going to write
		$producer config produce.offset.report false
		set topicIndex $topic.index
		set indexTopicObj [$producer new_topic #auto $topicIndex]

		# now start requesting a delivery report "sample",
		# i.e. one
		periodically_sample_writer

	}

	#
	# produce - write to the topic, the key is the clock
	#
	method produce {payload clock} {
		$topicObj produce $partition $payload $clock
	}

	#
	# dr_callback - delivery report callback.  the callback is a list of
	#   key-value pairs.  grab that into an array and write into the
	#   index partition the offset as the payload and the key as the key
	#
	method dr_callback {list} {
		array set row $list

		$indexTopicObj produce $partition $row(offset) $row(key)
		logger "produced $row(offset) $row(key) to index"
	}

	#
	# periodically_sample_writer - set to receive the next delivery report
	#   for the topic being written
	#
	method periodically_sample_writer {} {
		logger "periodically_sample_writer fired"
		after $sampleFrequencyMS [list $this periodically_sample_writer]

		$master delivery_report sample
	}

	method logger {message} {
		puts stderr "$this: $message"
	}
}

# vim: set ts=4 sw=4 sts=4 noet :

