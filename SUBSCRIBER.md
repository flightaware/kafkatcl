* **kafka::subscriber** ?command-name?

* **kafka::subscriber** #auto

 Create a kafkatcl subscription-consuming command ("subscriber") named *command-name*


## subscribers

 This uses the new subscription based API that allows it to follow multiple topics and multiple consumers are load-balanced between the subscribers automatically.

* subscriber subscribe ?topic-list?

Requests events from the named topics. Topics may be regexps if they start with "^". Partitions will be dynamically assigned to this subscriber by librdkafka.

* subscriber unsubscribe

Remove the current subsciption list.

* subscriber assignments

List of topics and partitions assigned to this subscriber.

* subscriber assign ?topic-partition-list?

Manually override the assignment or remove it completely with a null assignment or assiging the dummy topic "#none".

* subscriber consume

Return the next event from the subscription as a key-value Tcl list.

* subscriber callback ?function?

Set a callback function to be passed events from this subscription in the background.

* subscriber commit ?topic-partition-offset-list?

Commit the listed tuples.

###topic-partition-offset-list

This is a list of tuples, {{topic partition offset} {topic partition offset} ...}

The offset is required for "commit", and may be left out or set to zero for assign

The partition is required for "assign", but shoudl be left out or set to zero for subscribe.
