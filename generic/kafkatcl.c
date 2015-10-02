/*
 * kafkatcl - Tcl interface to Apache Kafka
 *
 * Copyright (C) 2015 FlightAware LLC
 *
 * freely redistributable under the Berkeley license
 */

#include "kafkatcl.h"
#include <assert.h>
#include <stdlib.h>

Tcl_Obj *kafkatcl_loggingCallbackObj = NULL;
Tcl_ThreadId kafkatcl_loggingCallbackThreadId = NULL;
Tcl_Interp *loggingInterp = NULL;

int
kafkatcl_check_consumer_callbacks (kafkatcl_objectClientData *ko);

void
kafkatcl_consume_stop_all_partitions (kafkatcl_topicClientData *kt);

// add a way to marshall a list of partitions being consumed
// add a way to stop all consumers
// stop all topic consumers when deleting a topic
// delete all running consumer objects when deleting a topic

/*
 *--------------------------------------------------------------
 *
 * kafkatcl_kafkaObjectDelete -- command deletion callback routine.
 *
 * Results:
 *      ...destroys the kafka connection object.
 *      ...frees memory.
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
void
kafkatcl_kafkaObjectDelete (ClientData clientData)
{
    kafkatcl_objectClientData *ko = (kafkatcl_objectClientData *)clientData;

    assert (ko->kafka_object_magic == KAFKA_OBJECT_MAGIC);

	rd_kafka_conf_destroy (ko->conf);
	rd_kafka_topic_conf_destroy (ko->topicConf);
    ckfree((char *)clientData);
}

/*
 *--------------------------------------------------------------
 *
 * kafkatcl_topicObjectDelete -- command deletion callback routine.
 *
 * Results:
 *      ...destroys the topic object.
 *      ...frees memory.
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
void
kafkatcl_topicObjectDelete (ClientData clientData)
{
    kafkatcl_topicClientData *kt = (kafkatcl_topicClientData *)clientData;

    assert (kt->kafka_topic_magic == KAFKA_TOPIC_MAGIC);

	kafkatcl_consume_stop_all_partitions (kt);

	rd_kafka_topic_destroy (kt->rkt);

	// free the topic name
	ckfree (kt->topic);

	// remove the topic instance from the list of topic consumers
	KT_LIST_REMOVE (kt, topicConsumerInstance);

    ckfree((char *)clientData);
}

/*
 *--------------------------------------------------------------
 *
 * kafkatcl_handleObjectDelete -- command deletion callback routine.
 *
 * Results:
 *      ...destroys the handle object.
 *      ...frees memory.
 *
 * Side effects:
 *      None.
 *
 *
 *      NB IF YOU DESTROY THE HANDLE YOU HAVE TO DESTROY THE TOPICS AND THE QUEUES
 *
 *--------------------------------------------------------------
 */
void
kafkatcl_handleObjectDelete (ClientData clientData)
{
    kafkatcl_handleClientData *kh = (kafkatcl_handleClientData *)clientData;

    assert (kh->kafka_handle_magic == KAFKA_HANDLE_MAGIC);

	rd_kafka_destroy (kh->rk);

	// destroy metadata if it exists
	if (kh->metadata != NULL) {
		rd_kafka_metadata_destroy (kh->metadata);
	}

	rd_kafka_topic_conf_destroy (kh->topicConf);

    ckfree((char *)clientData);
}

/*
 *--------------------------------------------------------------
 *
 * kafkatcl_queueObjectDelete -- command deletion callback routine.
 *
 * Results:
 *      ...destroys the handle object.
 *      ...frees memory.
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
void
kafkatcl_queueObjectDelete (ClientData clientData)
{
    kafkatcl_queueClientData *kq = (kafkatcl_queueClientData *)clientData;

    assert (kq->kafka_queue_magic == KAFKA_QUEUE_MAGIC);

	rd_kafka_queue_destroy (kq->rkqu);

	// if we have a running consumer on this queue, free its structure
	if (kq->krc != NULL) {
		ckfree (kq->krc);
	}

	KT_LIST_REMOVE (kq, queueConsumerInstance);

    ckfree((char *)clientData);
}

/*
 *--------------------------------------------------------------
 *
 * kafkatcl_parse_offset -- parse an offset from tcl, can be a straight up
 *   number.
 *
 *   or
 *
 *   beginning
 *   end
 *   stored
 *
 *   number
 *
 *   if the number is positive, it's an offset count.  if negative,
 *   it's a negative offset from the end
 *
 * Results:
 *      ...destroys the handle object.
 *      ...frees memory.
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
int
kafkatcl_parse_offset (Tcl_Interp *interp, Tcl_Obj *offsetObj, int64_t *offsetPtr) {
	Tcl_WideInt offsetCount;
	int optionIndex;

	// use NULL for interp because we don't need an error message if
	// the int conversion fails -- we want to try for some strings
	// afterwards
	if (Tcl_GetWideIntFromObj (NULL, offsetObj, &offsetCount) == TCL_OK) {
		if (offsetCount < 0)  {
			*offsetPtr = (RD_KAFKA_OFFSET_TAIL(-offsetCount));
			return TCL_OK;
		}
		*offsetPtr = offsetCount;
		return TCL_OK;
	}

    static CONST char *optionStrings[] = {
        "beginning",
        "end",
        "stored",
        NULL
    };

    enum options {
        OPT_BEGINNING,
        OPT_END,
        OPT_STORED
	};

    // argument must be one of the options defined above
    if (Tcl_GetIndexFromObj (interp, offsetObj, optionStrings, "offsetString",
        TCL_EXACT, &optionIndex) != TCL_OK) {
		Tcl_AppendResult (interp, " or a wide integer", NULL);
        return TCL_ERROR;
    }

    switch ((enum options) optionIndex) {
        case OPT_BEGINNING: {
			*offsetPtr = RD_KAFKA_OFFSET_BEGINNING;
			break;
		}

        case OPT_END: {
			*offsetPtr = RD_KAFKA_OFFSET_END;
			break;
		}

        case OPT_STORED: {
			*offsetPtr = RD_KAFKA_OFFSET_STORED;
			break;
		}
	}

	return TCL_OK;
}

/*
 *--------------------------------------------------------------
 *
 * kafkatcl_kafka_error_to_errorcode_string -- given a kafka
 *   code return a string corresponding to the kafka error constant
 *
 * Results:
 *      returns a pointer to a const char *
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
const char *kafkatcl_kafka_error_to_errorcode_string (rd_kafka_resp_err_t kafkaError)
{
	switch (kafkaError) {
		case RD_KAFKA_RESP_ERR_UNKNOWN:
			return "RD_KAFKA_RESP_ERR_UNKNOWN";

		case RD_KAFKA_RESP_ERR_NO_ERROR:
			return "RD_KAFKA_RESP_ERR_NO_ERROR";

		case RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE:
			return "RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE";

		case RD_KAFKA_RESP_ERR_INVALID_MSG:
			return "RD_KAFKA_RESP_ERR_INVALID_MSG";

		case RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART:
			return "RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART";

		case RD_KAFKA_RESP_ERR_INVALID_MSG_SIZE:
			return "RD_KAFKA_RESP_ERR_INVALID_MSG_SIZE";

		case RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE:
			return "RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE";

		case RD_KAFKA_RESP_ERR_NOT_LEADER_FOR_PARTITION:
			return "RD_KAFKA_RESP_ERR_NOT_LEADER_FOR_PARTITION";

		case RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT:
			return "RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT";

		case RD_KAFKA_RESP_ERR_BROKER_NOT_AVAILABLE:
			return "RD_KAFKA_RESP_ERR_BROKER_NOT_AVAILABLE";

		case RD_KAFKA_RESP_ERR_REPLICA_NOT_AVAILABLE:
			return "RD_KAFKA_RESP_ERR_REPLICA_NOT_AVAILABLE";

		case RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE:
			return "RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE";

		case RD_KAFKA_RESP_ERR_STALE_CTRL_EPOCH:
			return "RD_KAFKA_RESP_ERR_STALE_CTRL_EPOCH";

		case RD_KAFKA_RESP_ERR_OFFSET_METADATA_TOO_LARGE:
			return "RD_KAFKA_RESP_ERR_OFFSET_METADATA_TOO_LARGE";

		case RD_KAFKA_RESP_ERR_OFFSETS_LOAD_IN_PROGRESS:
			return "RD_KAFKA_RESP_ERR_OFFSETS_LOAD_IN_PROGRESS";

		case RD_KAFKA_RESP_ERR_CONSUMER_COORDINATOR_NOT_AVAILABLE:
			return "RD_KAFKA_RESP_ERR_CONSUMER_COORDINATOR_NOT_AVAILABLE";

		case RD_KAFKA_RESP_ERR_NOT_COORDINATOR_FOR_CONSUMER:
			return "RD_KAFKA_RESP_ERR_NOT_COORDINATOR_FOR_CONSUMER";

		default:
			return "RD_KAFKA_UNRECOGNIZED_ERROR";
	}
	return NULL;
}

/*
 *--------------------------------------------------------------
 *
 * kafkatcl_obj_to_kafka_log_level -- lookup a string in a Tcl object
 *   to be one of the log level strings for logLevel and set
 *   a pointer to a passed-in logLevel value to the corresponding
 *   logLevel such as LOG_WARNING, etc
 *
 * Results:
 *      ...kafka log level gets set
 *      ...a standard Tcl result is returned
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
int
kafkatcl_obj_to_log_level (Tcl_Interp *interp, Tcl_Obj *tclObj, int *logLevel) {
    int                 logIndex;

    static CONST char *logLevels[] = {
        "emerg",
        "alert",
        "crit",
        "err",
        "warning",
        "notice",
        "info",
        "debug",
        NULL
    };

    enum loglevels {
        OPT_EMERG,
        OPT_ALERT,
        OPT_CRIT,
		OPT_ERR,
        OPT_WARNING,
        OPT_NOTICE,
        OPT_INFO,
		OPT_DEBUG
	};

    // argument must be one of the options defined above
    if (Tcl_GetIndexFromObj (interp, tclObj, logLevels, "logLevel",
        TCL_EXACT, &logIndex) != TCL_OK) {
        return TCL_ERROR;
    }

    switch ((enum loglevels) logIndex) {
        case OPT_EMERG: {
			*logLevel = LOG_EMERG;
			break;
		}

        case OPT_ALERT: {
			*logLevel = LOG_ALERT;
			break;
		}

        case OPT_CRIT: {
			*logLevel = LOG_CRIT;
			break;
		}

        case OPT_ERR: {
			*logLevel = LOG_ERR;
			break;
		}

        case OPT_WARNING: {
			*logLevel = LOG_WARNING;
			break;
		}

        case OPT_NOTICE: {
			*logLevel = LOG_NOTICE;
			break;
		}

        case OPT_INFO: {
			*logLevel = LOG_INFO;
			break;
		}

		case OPT_DEBUG: {
			*logLevel = LOG_DEBUG;
			break;
		}
	}
	return TCL_OK;
}

/*
 *--------------------------------------------------------------
 *
 * kafkatcl_log_level_to_string -- given a log level,
 *   return a const char * to a character string of equivalent
 *   meaning
 *
 * Results:
 *      a string gets returned
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
const char *
kafkatcl_log_level_to_string (int severity) {
	switch (severity) {
		case LOG_EMERG:
			return "emerg";

		case LOG_ALERT:
			return "alert";

		case LOG_CRIT:
			return "crit";

		case LOG_ERR:
			return "err";

		case LOG_WARNING:
			return "warning";

		case LOG_NOTICE:
			return "notice";

		case LOG_INFO:
			return "info";

		case LOG_DEBUG:
			return "debug";

		default:
			return "unknown";
	}
}

/*
 *--------------------------------------------------------------
 *
 * kafkatcl_kafka_error_to_tcl -- given a kafka error code and a field
 *   name, if the error code is RD_KAFKA_RESP_ERR_NO_ERROR then
 *   return TCL_OK but if it's anything
 *   else, set the interpreter result to the corresponding error string
 *   and set the error code to KAFKA and the e-code like
 *   RD_KAFKA_RESP_ERR_INVALID_MSG
 *
 * Results:
 *      A standard Tcl result
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
int kafkatcl_kafka_error_to_tcl (Tcl_Interp *interp, rd_kafka_resp_err_t kafkaError, char *string) {

	if (kafkaError == RD_KAFKA_RESP_ERR_NO_ERROR) {
		return TCL_OK;
	}

	const char *kafkaErrorString = rd_kafka_err2str (kafkaError);

	const char *kafkaErrorCodeString = kafkatcl_kafka_error_to_errorcode_string (kafkaError);

	Tcl_ResetResult (interp);
	Tcl_SetErrorCode (interp, "KAFKA", kafkaErrorCodeString, kafkaErrorString, string, NULL);
	Tcl_AppendResult (interp, "kafka error: ", kafkaErrorString, NULL);

	if (string != NULL && *string != '\0') {
		Tcl_AppendResult (interp, " (", kafkaErrorString, ")", NULL);
	}
	return TCL_ERROR;
}

/*
 *--------------------------------------------------------------
 *
 * kafkatcl_errorno_to_tcl_error -- some kafka library routines use errno
 *   to communicate errors.  We can use rd_kafka_errno2err to convert
 *   those to a more standard kafka error
 *
 * Results:
 *      A standard Tcl result
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
int
kafktcl_errno_to_tcl_error (Tcl_Interp *interp) {
	int myErrno = Tcl_GetErrno ();
	rd_kafka_resp_err_t kafkaError = rd_kafka_errno2err (myErrno);
	return kafkatcl_kafka_error_to_tcl (interp, kafkaError, NULL);
}

/*
 *--------------------------------------------------------------
 *
 * kafkatcl_conf_to_array -- given an interp, an array name and
 *   a rd_kafka_conf_t, populate the array with the elements of
 *   the rd_kafka_conf_t
 *
 * Results:
 *      A standard Tcl result
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
int kafkatcl_conf_to_array (Tcl_Interp *interp, char *arrayName, rd_kafka_conf_t *conf) {
	size_t count;
	const char **stringPairs = rd_kafka_conf_dump (conf, &count);
	int resultCode = TCL_OK;
	int i;

	for (i = 0; i < count; i += 2) {
		if (Tcl_SetVar2 (interp, arrayName, stringPairs[i], stringPairs[i+1], (TCL_LEAVE_ERR_MSG)) == NULL) {
			resultCode = TCL_ERROR;
			break;
		}
	}
	rd_kafka_conf_dump_free (stringPairs, count);
	return resultCode;
}

/*
 *--------------------------------------------------------------
 *
 * kafkatcl_topic_conf_to_array -- given an interp, an array name and
 *   a rd_kafka_topic_conf_t, populate the array with the elements of
 *   the rd_kafka_topic_conf_t
 *
 * Results:
 *      A standard Tcl result
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
int kafkatcl_topic_conf_to_array (Tcl_Interp *interp, char *arrayName, rd_kafka_topic_conf_t *topicConf) {
	size_t count;
	const char **stringPairs = rd_kafka_topic_conf_dump (topicConf, &count);
	int resultCode = TCL_OK;
	int i;

	for (i = 0; i < count; i += 2) {
		if (Tcl_SetVar2 (interp, arrayName, stringPairs[i], stringPairs[i+1], (TCL_LEAVE_ERR_MSG)) == NULL) {
			resultCode = TCL_ERROR;
			break;
		}
	}
	rd_kafka_conf_dump_free (stringPairs, count);
	return resultCode;
}

/*
 *--------------------------------------------------------------
 *
 * kafkatcl_stringpairs_to_tcl_list -- given a list of pointers
 *   to strings and a count of the number, return a Tcl list object
 *
 * Results:
 *      A standard Tcl result
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
Tcl_Obj *
kafkatcl_stringpairs_to_tcl_list (const char **stringPairs, size_t count) {
	Tcl_Obj *listObj;

	Tcl_Obj **objv = (Tcl_Obj **)ckalloc (sizeof(Tcl_Obj *) * count);
	int i;

	for (i = 0; i < count; i++) {
		objv[i] = Tcl_NewStringObj (stringPairs[i], -1);
	}

	listObj = Tcl_NewListObj (count, objv);
	ckfree ((char *)objv);
	return listObj;
}

/*
 *--------------------------------------------------------------
 *
 * kafkatcl_conf_to_list -- given an interp and
 *   a rd_kafka_conf_t, return a list composed of
 *   the elements of the rd_kafka_conf_t
 *
 * Results:
 *      A standard Tcl result
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
int kafkatcl_conf_to_list (Tcl_Interp *interp, rd_kafka_conf_t *conf) {
	size_t count;
	const char **stringPairs = rd_kafka_conf_dump (conf, &count);

	Tcl_SetObjResult (interp, kafkatcl_stringpairs_to_tcl_list (stringPairs, count));
	rd_kafka_conf_dump_free (stringPairs, count);

	return TCL_OK;
}

/*
 *--------------------------------------------------------------
 *
 * kafkatcl_topic_conf_to_list -- given an interp and
 *   a rd_kafka_topic_conf_t, return a list composed from
 *   the key-value pairs of the rd_kafka_topic_conf_t
 *
 * Results:
 *      A standard Tcl result
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
int kafkatcl_topic_conf_to_list (Tcl_Interp *interp, rd_kafka_topic_conf_t *topicConf) {
	size_t count;
	const char **stringPairs = rd_kafka_topic_conf_dump (topicConf, &count);

	Tcl_SetObjResult (interp, kafkatcl_stringpairs_to_tcl_list (stringPairs, count));

	rd_kafka_conf_dump_free (stringPairs, count);
	return TCL_OK;
}


/*
 *--------------------------------------------------------------
 *
 *   kafkatcl_topic_command_to_topicClientData -- given a topic command name,
 *   find it in the interpreter and return a pointer to its topic client
 *   data or NULL
 *
 * Results:
 *     returns NULL or the pointer
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
kafkatcl_topicClientData *
kafkatcl_topic_command_to_topicClientData (Tcl_Interp *interp, char *topicCommandName)
{
	Tcl_CmdInfo topicCmdInfo;

	if (!Tcl_GetCommandInfo (interp, topicCommandName, &topicCmdInfo)) {
		return NULL;
	}

	kafkatcl_topicClientData *kt = (kafkatcl_topicClientData *)topicCmdInfo.objClientData;
    if (kt->kafka_topic_magic != KAFKA_TOPIC_MAGIC) {
		return NULL;
	}

	return kt;
}

/*
 *--------------------------------------------------------------
 *
 *   kafkatcl_handle_command_to_handleClientData -- given a handle command name,
 *   find it in the interpreter and return a pointer to its handle client
 *   data or NULL
 *
 * Results:
 *     returns NULL or the pointer
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
kafkatcl_handleClientData *
kafkatcl_handle_command_to_handleClientData (Tcl_Interp *interp, char *handleCommandName)
{
	Tcl_CmdInfo handleCmdInfo;

	if (!Tcl_GetCommandInfo (interp, handleCommandName, &handleCmdInfo)) {
		return NULL;
	}

	kafkatcl_handleClientData *kh = (kafkatcl_handleClientData *)handleCmdInfo.objClientData;
    if (kh->kafka_handle_magic != KAFKA_HANDLE_MAGIC) {
		return NULL;
	}

	return kh;
}

/*
 *--------------------------------------------------------------
 *
 *   kafkatcl_message_to_tcl_list -- given a Tcl interpreter,
 *   and a kafka rd_kafka_message_t message, generate
 *   a list of key value pairs of the message payload, partition,
 *   key, offset and topic or generate an error key-value pair
 *
 * Results:
 *     a standard Tcl result
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
Tcl_Obj *
kafkatcl_message_to_tcl_list (Tcl_Interp *interp, rd_kafka_message_t *rdm) {
	Tcl_Obj *listObj;

	if (rdm->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
		return NULL;
	}

	if (rdm->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
		// error message is in the payload

		const char *kafkaErrorString = rd_kafka_err2str (rdm->err);
		const char *kafkaErrorCodeString = kafkatcl_kafka_error_to_errorcode_string (rdm->err);

#define KAFKATCL_MESSAGE_ERROR_LIST_COUNT 6
		Tcl_Obj *listObjv[KAFKATCL_MESSAGE_ERROR_LIST_COUNT];

		listObjv[0] = Tcl_NewStringObj ("error", -1);
		listObjv[1] = Tcl_NewStringObj (kafkaErrorString, -1);

		listObjv[2] = Tcl_NewStringObj ("code", -1);
		listObjv[3] = Tcl_NewStringObj (kafkaErrorCodeString, -1);

		listObjv[4] = Tcl_NewStringObj ("message", -1);
		listObjv[5] = Tcl_NewStringObj (rdm->payload, rdm->len);

		listObj = Tcl_NewListObj (KAFKATCL_MESSAGE_ERROR_LIST_COUNT, listObjv);
	} else {
#define KAFKATCL_GOOD_MESSAGE_LIST_COUNT 10
		Tcl_Obj *listObjv[KAFKATCL_GOOD_MESSAGE_LIST_COUNT];
		int i = 0;

		listObjv[i++] = Tcl_NewStringObj ("payload", -1);
		listObjv[i++] = Tcl_NewStringObj (rdm->payload, rdm->len);

		listObjv[i++] = Tcl_NewStringObj ("partition", -1);
		listObjv[i++] = Tcl_NewIntObj (rdm->partition);

		listObjv[i++] = Tcl_NewStringObj ("offset", -1);
		listObjv[i++] = Tcl_NewWideIntObj (rdm->offset);

		// include the topic name if there is a topic structure
		if (rdm->rkt != NULL) {
			listObjv[i++] = Tcl_NewStringObj ("topic", -1);
			listObjv[i++] = Tcl_NewStringObj (rd_kafka_topic_name (rdm->rkt), -1);
		}

		// add the key if there is one
		if (rdm->key != NULL) {
			listObjv[i++] = Tcl_NewStringObj ("key", -1);
			listObjv[i++] = Tcl_NewStringObj (rdm->key, rdm->key_len);
		}

		assert (i <= KAFKATCL_GOOD_MESSAGE_LIST_COUNT);

		listObj = Tcl_NewListObj (i, listObjv);
	}

	return listObj;
}


void
kafkatcl_unset_response_elements (Tcl_Interp *interp, char *arrayName) {
	Tcl_UnsetVar2 (interp, arrayName, "payload", 0);
	Tcl_UnsetVar2 (interp, arrayName, "partition", 0);
	Tcl_UnsetVar2 (interp, arrayName, "key", 0);
	Tcl_UnsetVar2 (interp, arrayName, "offset", 0);
	Tcl_UnsetVar2 (interp, arrayName, "topic", 0);
}

/*
 *--------------------------------------------------------------
 *
 *   kafkatcl_message_to_tcl_array -- given a Tcl interpreter, the name of
 *   an array and a kafka rd_kafka_message_t message, either generate
 *   a tcl error or set fields of the message into the specified array
 *
 * Results:
 *     a standard Tcl result
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
int
kafkatcl_message_to_tcl_array (Tcl_Interp *interp, char *arrayName, rd_kafka_message_t *rdm) {
	if (rdm->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
		// error message is in the payload
		kafkatcl_unset_response_elements (interp, arrayName);

		if (rdm->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
			return TCL_BREAK;
		}

		return kafkatcl_kafka_error_to_tcl (interp, rdm->err, (char *)rdm->payload);
	}

	Tcl_Obj *payloadObj = Tcl_NewByteArrayObj (rdm->payload, rdm->len);
	if (Tcl_SetVar2Ex (interp, arrayName, "payload", payloadObj, (TCL_LEAVE_ERR_MSG)) == NULL) {
		return TCL_ERROR;
	}

	Tcl_Obj *partitionObj = Tcl_NewIntObj (rdm->partition);
	if (Tcl_SetVar2Ex (interp, arrayName, "partition", partitionObj, (TCL_LEAVE_ERR_MSG)) == NULL) {
		return TCL_ERROR;
	}

	if (rdm->key != NULL) {
		Tcl_Obj *keyObj = Tcl_NewByteArrayObj (rdm->key, rdm->key_len);
		if (Tcl_SetVar2Ex (interp, arrayName, "key", keyObj, (TCL_LEAVE_ERR_MSG)) == NULL) {
			return TCL_ERROR;
		}
	}

	Tcl_Obj *offsetObj = Tcl_NewWideIntObj (rdm->offset);
	if (Tcl_SetVar2Ex (interp, arrayName, "offset", offsetObj, (TCL_LEAVE_ERR_MSG)) == NULL) {
		return TCL_ERROR;
	}


	Tcl_Obj *topicObj = Tcl_NewStringObj (rd_kafka_topic_name (rdm->rkt), -1);
	if (Tcl_SetVar2Ex (interp, arrayName, "topic", topicObj, (TCL_LEAVE_ERR_MSG)) == NULL) {
		return TCL_ERROR;
	}

	return TCL_OK;
}

/*
 *--------------------------------------------------------------
 *
 *   kafkatcl_queue_command_to_queueClientData -- given a queue command name,
 *   find it in the interpreter and return a pointer to its queue client
 *   data or NULL
 *
 * Results:
 *     returns NULL or the pointer
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
kafkatcl_queueClientData *
kafkatcl_queue_command_to_queueClientData (Tcl_Interp *interp, char *queueCommandName)
{
	Tcl_CmdInfo queueCmdInfo;

	if (!Tcl_GetCommandInfo (interp, queueCommandName, &queueCmdInfo)) {
		return NULL;
	}

	kafkatcl_queueClientData *kq = (kafkatcl_queueClientData *)queueCmdInfo.objClientData;
    if (kq->kafka_queue_magic != KAFKA_QUEUE_MAGIC) {
		return NULL;
	}

	return kq;
}

/*
 *--------------------------------------------------------------
 *
 * kafkatcl_invoke_callback_with_argument --
 *
 *     The twist here is that a callback object might be a list, not
 *     just a command name, like the argument to -callback might be
 *     more than just a function name, like it could be an object name
 *     and a method name and an argument or whatever.
 *
 *     This code splits out that list and generates up an eval thingie
 *     and invokes it with the additional argument tacked onto the end,
 *     a future object or the like.
 *
 * Results:
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
int
kafkatcl_invoke_callback_with_argument (Tcl_Interp *interp, Tcl_Obj *callbackObj, Tcl_Obj *argumentObj) {
	int callbackListObjc;
	Tcl_Obj **callbackListObjv;
	int tclReturnCode;

	int evalObjc;
	Tcl_Obj **evalObjv;

	int i;

	if (Tcl_ListObjGetElements (interp, callbackObj, &callbackListObjc, &callbackListObjv) == TCL_ERROR) {
		Tcl_AppendResult (interp, " while converting callback argument", NULL);
		return TCL_ERROR;
	}

	evalObjc = callbackListObjc + 1;
	evalObjv = (Tcl_Obj **)ckalloc (sizeof (Tcl_Obj *) * evalObjc);

	for (i = 0; i < callbackListObjc; i++) {
		evalObjv[i] = callbackListObjv[i];
		Tcl_IncrRefCount (evalObjv[i]);
	}

	evalObjv[evalObjc - 1] = argumentObj;
	Tcl_IncrRefCount (evalObjv[evalObjc - 1]);

	tclReturnCode = Tcl_EvalObjv (interp, evalObjc, evalObjv, (TCL_EVAL_GLOBAL|TCL_EVAL_DIRECT));

	// if we got a Tcl error, since we initiated the event, it doesn't
	// have anything to traceback further from here to, we must initiate
	// a background error, which will generally cause the bgerror proc
	// to get invoked
	if (tclReturnCode == TCL_ERROR) {
		Tcl_BackgroundException (interp, TCL_ERROR);
	}

	for (i = 0; i < evalObjc; i++) {
		Tcl_DecrRefCount (evalObjv[i]);
	}

	ckfree ((char *)evalObjv);
	return tclReturnCode;
}

/*
 *----------------------------------------------------------------------
 *
 * kafkatcl_EventSetupProc --
 *    This routine is a required argument to Tcl_CreateEventSource
 *
 *    Since we need to poll librdkafka to get events to fire, let's
 *    make sure we get called periodically
 *
 *
 * Results:
 *    Our polling routine will get called periodically.
 *
 *----------------------------------------------------------------------
 */
void
kafkatcl_EventSetupProc (ClientData clientData, int flags) {
	Tcl_Time time = {0, 100000};

	Tcl_SetMaxBlockTime (&time);
}

/*
 *----------------------------------------------------------------------
 *
 * kafkatcl_EventCheckProc --
 *
 *    This is a function we pass to Tcl_CreateEventSource that is
 *    invoked to see if any events have occurred and to queue them.
 *
 *    rdkafkalib requires that we invoke a poll function to trigger
 *    the kafka-provided callbacks to be invoked.  So we do that.
 *
 * Results:
 *    The program compiles.
 *
 *----------------------------------------------------------------------
 */
void
kafkatcl_EventCheckProc (ClientData clientData, int flags) {
    kafkatcl_handleClientData *kh = (kafkatcl_handleClientData *)clientData;

	// polling with timeoutMS of 0 is nonblocking, which is ideal
	rd_kafka_poll (kh->rk, 0);
	kafkatcl_check_consumer_callbacks (kh->ko);
}

/*
 *----------------------------------------------------------------------
 *
 * kafkatcl_logging_eventProc --
 *
 *    this routine is called by the Tcl event handler to process logging
 *    callbacks we have gotten from the Kafka cpp-driver
 *
 * Results:
 *    returns 1 to say we handled the event and the dispatcher can delete it
 *
 *----------------------------------------------------------------------
 */
int
kafkatcl_logging_eventProc (Tcl_Event *tevPtr, int flags) {

	// we got called with a Tcl_Event pointer but really it's a pointer to
	// our kafkatcl_loggingEvent structure that has the Tcl_Event and
	// some other stuff that we need.
	// Go get that.

	kafkatcl_loggingEvent *evPtr = (kafkatcl_loggingEvent *)tevPtr;
	Tcl_Interp *interp = evPtr->interp;
#define KAFKATCL_LOG_CALLBACK_LISTCOUNT 6

	Tcl_Obj *listObjv[KAFKATCL_LOG_CALLBACK_LISTCOUNT];

	// probably won't happen but if we get a logging callback and have
	// no callback object, return 1 saying we handled it and let the
	// dispatcher delete the message NB this isn't exactly cool
	if (kafkatcl_loggingCallbackObj == NULL) {
		return 1;
	}

	// construct a list of key-value pairs representing the log message

	listObjv[0] = Tcl_NewStringObj ("level", -1);
	listObjv[1] = Tcl_NewIntObj (evPtr->level);

	listObjv[2] = Tcl_NewStringObj ("facility", -1);
	listObjv[3] = Tcl_NewStringObj (evPtr->fac, -1);

	listObjv[4] = Tcl_NewStringObj ("message", -1);
	listObjv[5] = Tcl_NewStringObj (evPtr->buf, -1);


	Tcl_Obj *listObj = Tcl_NewListObj (KAFKATCL_LOG_CALLBACK_LISTCOUNT, listObjv);

	ckfree (evPtr->fac);
	evPtr->fac = NULL;

	ckfree (evPtr->buf);
	evPtr->buf = NULL;

	// even if this fails we still want the event taken off the queue
	// this function will do the background error thing if there is a tcl
	// error running the callback
	kafkatcl_invoke_callback_with_argument (interp, kafkatcl_loggingCallbackObj, listObj);
	// tell the dispatcher we handled it.  0 would mean we didn't deal with
	// it and don't want it removed from the queue
	return 1;
}

/*
 *----------------------------------------------------------------------
 *
 * kafkatcl_stats_eventProc --
 *
 *    this routine is called by the Tcl event handler to process stats
 *    callbacks we have gotten from the Kafka cpp-driver
 *
 * Results:
 *    returns 1 to say we handled the event and the dispatcher can delete it
 *
 *----------------------------------------------------------------------
 */
int
kafkatcl_stats_eventProc (Tcl_Event *tevPtr, int flags) {

	// we got called with a Tcl_Event pointer but really it's a pointer to
	// our kafkatcl_statsEvent structure that has the Tcl_Event and
	// some other stuff that we need.
	// Go get that.

	kafkatcl_statsEvent *evPtr = (kafkatcl_statsEvent *)tevPtr;
	kafkatcl_objectClientData *ko = evPtr->ko;
	Tcl_Interp *interp = ko->interp;

	Tcl_Obj *jsonObj = Tcl_NewStringObj (evPtr->json, evPtr->jsonLen);


	// even if this fails we still want the event taken off the queue
	// this function will do the background error thing if there is a tcl
	// error running the callback
	kafkatcl_invoke_callback_with_argument (interp, ko->statisticsCallbackObj, jsonObj);
	free (evPtr->json);
	// tell the dispatcher we handled it.  0 would mean we didn't deal with
	// it and don't want it removed from the queue
	return 1;
}

/*
 *----------------------------------------------------------------------
 *
 * kafkatcl_error_eventProc --
 *
 *    this routine is called by the Tcl event handler to process error
 *    callbacks we have gotten from the Kafka cpp-driver
 *
 * Results:
 *    returns 1 to say we handled the event and the dispatcher can delete it
 *
 *----------------------------------------------------------------------
 */
int
kafkatcl_error_eventProc (Tcl_Event *tevPtr, int flags) {

	// we got called with a Tcl_Event pointer but really it's a pointer to
	// our kafkatcl_statsEvent structure that has the Tcl_Event and
	// some other stuff that we need.
	// Go get that.

	kafkatcl_errorEvent *evPtr = (kafkatcl_errorEvent *)tevPtr;
	kafkatcl_objectClientData *ko = evPtr->ko;
	Tcl_Interp *interp = ko->interp;

#define KAFKATCL_EVENT_CALLBACK_LISTCOUNT 4

	Tcl_Obj *listObjv[KAFKATCL_LOG_CALLBACK_LISTCOUNT];

	// construct a list of key-value pairs representing the log message

	listObjv[0] = Tcl_NewStringObj ("err", -1);
	listObjv[1] = Tcl_NewIntObj (evPtr->err);

	listObjv[2] = Tcl_NewStringObj ("reason", -1);
	listObjv[3] = Tcl_NewStringObj (evPtr->reason, -1);


	Tcl_Obj *listObj = Tcl_NewListObj (KAFKATCL_EVENT_CALLBACK_LISTCOUNT, listObjv);

	// even if this fails we still want the event taken off the queue
	// this function will do the background error thing if there is a tcl
	// error running the callback
	kafkatcl_invoke_callback_with_argument (interp, ko->errorCallbackObj, listObj);

	// tell the dispatcher we handled it.  0 would mean we didn't deal with
	// it and don't want it removed from the queue
	return 1;
}

/*
 *----------------------------------------------------------------------
 *
 * kafkatcl_logging_callback --
 *
 *    this routine is called by the kafka cpp-driver as a callback
 *    when a log message has been received and rd_kafka_set_logger
 *    has been done to register this callback
 *
 * Results:
 *    an event is queued to the thread that started our conversation with
 *    kafka
 *
 *----------------------------------------------------------------------
 */
void kafkatcl_logging_callback (const rd_kafka_t *rk, int level, const char *fac, const char *buf) {
	kafkatcl_loggingEvent *evPtr;

	Tcl_Interp *interp = loggingInterp;

	evPtr = ckalloc (sizeof (kafkatcl_loggingEvent));
	evPtr->event.proc = kafkatcl_logging_eventProc;
	evPtr->interp = interp;

	evPtr->level = level;

	int len = strlen (fac);
	evPtr->fac = ckalloc (len + 1);
	strncpy (evPtr->fac, fac, len);

	len = strlen (buf);
	evPtr->buf = ckalloc (len + 1);
	strncpy (evPtr->buf, buf, len);

	Tcl_ThreadQueueEvent(kafkatcl_loggingCallbackThreadId, (Tcl_Event *)evPtr, TCL_QUEUE_TAIL);
}

/*
 *----------------------------------------------------------------------
 *
 * kafkatcl_error_callback --
 *
 *    this routine is called by the kafka cpp-driver as a callback
 *    when an error has been received and rd_kafka_set_error_cb
 *    has been done to register this callback
 *
 * Results:
 *    an event is queued to the thread that started our conversation with
 *    kafka
 *
 *----------------------------------------------------------------------
 */
void kafkatcl_error_callback (rd_kafka_t *rk, int err, const char *reason, void *opaque) {
	kafkatcl_objectClientData *ko = opaque;

	kafkatcl_errorEvent *evPtr;

	evPtr = ckalloc (sizeof (kafkatcl_errorEvent));

	evPtr->event.proc = kafkatcl_error_eventProc;
	evPtr->ko = ko;
	evPtr->err = err;
	evPtr->reason = reason;

	Tcl_ThreadQueueEvent (ko->threadId, (Tcl_Event *)evPtr, TCL_QUEUE_HEAD);
}

/*
 *----------------------------------------------------------------------
 *
 * kafkatcl_stats_callback --
 *
 *    this routine is called by the kafka cpp-driver as a callback
 *    when a stats message has been received and rd_kafka_set_stats_cb
 *    has been done to register this callback
 *
 * Results:
 *    an event is queued to the thread that started our conversation with
 *    kafka
 *
 *----------------------------------------------------------------------
 */
int kafkatcl_stats_callback (rd_kafka_t *rk, char  *json, size_t jsonLen, void *opaque) {
	kafkatcl_objectClientData *ko = opaque;

	kafkatcl_statsEvent *evPtr;

	evPtr = ckalloc (sizeof (kafkatcl_statsEvent));

	evPtr->event.proc = kafkatcl_stats_eventProc;
	evPtr->ko = ko;
	evPtr->json = json;
	evPtr->jsonLen = jsonLen;

	Tcl_ThreadQueueEvent (ko->threadId, (Tcl_Event *)evPtr, TCL_QUEUE_HEAD);
	// return 0 == free the json pointer immediately, else return 1
	return 1;
}

/*
 *----------------------------------------------------------------------
 *
 * kafkatcl_delivery_report_eventProc --
 *
 *    this routine is called by the Tcl event handler to process delivery
 *    report callbacks we have gotten from the Kafka cpp-driver
 *
 * Results:
 *    returns 1 to say we handled the event and the dispatcher can delete it
 *
 *----------------------------------------------------------------------
 */
int
kafkatcl_delivery_report_eventProc (Tcl_Event *tevPtr, int flags) {

	// we got called with a Tcl_Event pointer but really it's a pointer to
	// our kafkatcl_statsEvent structure that has the Tcl_Event and
	// some other stuff that we need.
	// Go get that.

	kafkatcl_deliveryReportEvent *evPtr = (kafkatcl_deliveryReportEvent *)tevPtr;
	kafkatcl_objectClientData *ko = evPtr->ko;
	Tcl_Interp *interp = ko->interp;

	Tcl_Obj *listObj = kafkatcl_message_to_tcl_list (interp, &evPtr->rkmessage);

	// free the payload
	ckfree (evPtr->rkmessage.payload);

	// free the key if there is one
	if (evPtr->rkmessage.key != NULL) {
		ckfree (evPtr->rkmessage.key);
	}

	// even if this fails we still want the event taken off the queue
	// this function will do the background error thing if there is a tcl
	// error running the callback
	kafkatcl_invoke_callback_with_argument (interp, ko->deliveryReportCallbackObj, listObj);

	// tell the dispatcher we handled it.  0 would mean we didn't deal with
	// it and don't want it removed from the queue
	return 1;
}


/*
 *----------------------------------------------------------------------
 *
 * kafkatcl_delivery_report_callback --
 *
 *    this routine is called by the kafka cpp-driver as a callback
 *    when a delivery report has been received and
 *    rd_kafka_set_dr_msg_cb has been done to register
 *    this callback
 *
 * Results:
 *    an event is queued to the thread that set up the callback
 *
 *----------------------------------------------------------------------
 */
void kafkatcl_delivery_report_callback (rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque) {
	kafkatcl_objectClientData *ko = opaque;

    assert (ko->kafka_object_magic == KAFKA_OBJECT_MAGIC);

	if (ko->sampleDeliveryReport) {
		ko->sampleDeliveryReport = 0;
	} else if (ko->deliveryReportEvery == 0) {
		return;
	} else {
		if (--ko->deliveryReportCountdown > 0) {
			return;
		}
		ko->deliveryReportCountdown = ko->deliveryReportEvery;
	}


	kafkatcl_deliveryReportEvent *evPtr;

	evPtr = ckalloc (sizeof (kafkatcl_deliveryReportEvent));

	evPtr->event.proc = kafkatcl_delivery_report_eventProc;
	evPtr->ko = ko;

	// structure copy
	evPtr->rkmessage = *rkmessage;

	// then allocate and copy the payload and possibly the key; we will free
	// all this in the event handler
	evPtr->rkmessage.payload = ckalloc (rkmessage->len);
	memcpy (evPtr->rkmessage.payload, rkmessage->payload, rkmessage->len);

	if (rkmessage->key != NULL) {
		evPtr->rkmessage.key = ckalloc (rkmessage->key_len);
		memcpy (evPtr->rkmessage.key, rkmessage->key, rkmessage->key_len);
	}

	Tcl_ThreadQueueEvent (ko->threadId, (Tcl_Event *)evPtr, TCL_QUEUE_HEAD);
	return;
}

/*
 *----------------------------------------------------------------------
 *
 * kafkatcl_statistics_callback --
 *
 *    this routine is called by the kafka cpp-driver as a callback
 *    when a delivery report has been received and rd_kafka_set_stats_cb
 *    has been done to register this callback
 *
 * Results:
 *    an event is queued to the thread that set up the callback
 *
 *----------------------------------------------------------------------
 */
void kafkatcl_statistics_callback (rd_kafka_t *rk, char *json, size_t json_len, void *opaque) {
    kafkatcl_objectClientData *ko = opaque;
	Tcl_Interp *interp = ko->interp;
	Tcl_Obj *arg = Tcl_NewStringObj (json, json_len);

	kafkatcl_invoke_callback_with_argument (interp, ko->statisticsCallbackObj, arg) ;
}

/*
 *----------------------------------------------------------------------
 *
 * kafkatcl_meta_topic_list --
 *
 *    given a handle client data create a tcl list of all of the topics
 *    and set the interpreter result to it if successful
 *
 * Results:
 *    a standard tcl result
 *
 *----------------------------------------------------------------------
 */
int
kafkatcl_meta_topic_list (kafkatcl_handleClientData *kh) {
	Tcl_Interp *interp = kh->interp;
	const struct rd_kafka_metadata *metadata = kh->metadata;
	int i;
	Tcl_Obj *listObj = Tcl_NewObj();

	for (i = 0 ; i < metadata->topic_cnt ; i++) {
		const struct rd_kafka_metadata_topic *t = &metadata->topics[i];

		if (Tcl_ListObjAppendElement (interp, listObj, Tcl_NewStringObj (t->topic, -1)) == TCL_ERROR) {
			return TCL_ERROR;
		}
	}
	Tcl_SetObjResult (interp, listObj);
	return TCL_OK;
}

/*
 *----------------------------------------------------------------------
 *
 * kafkatcl_meta_find_topic --
 *
 *    given a handle client data and a topic name,
 *    return the matching const struct rd_kafka_metadata_topic *
 *    pointer or NULL if none is found
 *
 * Results:
 *    the matching const struct rd_kafka_metadata_topic * or NULL
 *
 *----------------------------------------------------------------------
 */
const struct rd_kafka_metadata_topic *
kafkatcl_meta_find_topic (kafkatcl_handleClientData *kh, char *topic) {
	const struct rd_kafka_metadata *metadata = kh->metadata;
	int i;

	for (i = 0 ; i < metadata->topic_cnt ; i++) {
		const struct rd_kafka_metadata_topic *t = &metadata->topics[i];

		if (strcmp (t->topic, topic) == 0) {
			return t;
		}
	}

	return NULL;
}

/*
 *----------------------------------------------------------------------
 *
 * kafkatcl_meta_find_topic_tcl_result --
 *
 *    given a handle client data and a topic name,
 *    set the passed-in const struct rd_kafka_metadata_topic *
 *    to the matching metadata topic structure or to NULL if none is found
 *
 *    Set an error message into the Tcl interpreter if there is an
 *    error and return TCL_OK if the topic was found or TCL_ERROR if
 *    it wasn't
 *
 * Results:
 *    A standard Tcl result
 *
 *----------------------------------------------------------------------
 */
int
kafkatcl_meta_find_topic_tcl_result (kafkatcl_handleClientData *kh, char *topicName, const struct rd_kafka_metadata_topic **topicPtr) {
	const struct rd_kafka_metadata *metadata = kh->metadata;
	int i;
	Tcl_Interp *interp = kh->interp;

	for (i = 0 ; i < metadata->topic_cnt ; i++) {
		const struct rd_kafka_metadata_topic *t = &metadata->topics[i];

		if (strcmp (t->topic, topicName) == 0) {
			*topicPtr = t;
			return TCL_OK;
		}
	}

	*topicPtr = NULL;
	Tcl_ResetResult (interp);
	Tcl_AppendResult (interp, "kafka error: topic '", topicName, "' not found", NULL);
	return TCL_ERROR;
}

/*
 *----------------------------------------------------------------------
 *
 * kafkatcl_meta_topic_partitions --
 *
 *    given a handle client data and a topic name,
 *    set the interpreter result to the number of partitions
 *    covering that topic if successful
 *
 * Results:
 *    a standard tcl result
 *
 *----------------------------------------------------------------------
 */
int
kafkatcl_meta_topic_partitions (kafkatcl_handleClientData *kh, char *topic) {
	const struct rd_kafka_metadata_topic *t;
	Tcl_Interp *interp = kh->interp;

	if (kafkatcl_meta_find_topic_tcl_result (kh, topic, &t) == TCL_ERROR) {
		return TCL_ERROR;
	}
	Tcl_SetObjResult (interp, Tcl_NewIntObj (t->partition_cnt));
	return TCL_OK;
}

/*
 *----------------------------------------------------------------------
 *
 * kafkatcl_meta_broker_list --
 *
 *    given a handle client data create a tcl list of all of the brokers
 *    and set the interpreter result to it if successful
 *
 * Results:
 *    a standard tcl result
 *
 *----------------------------------------------------------------------
 */
int
kafkatcl_meta_broker_list (kafkatcl_handleClientData *kh) {
	Tcl_Interp *interp = kh->interp;
	const struct rd_kafka_metadata *metadata = kh->metadata;
	int i;
	Tcl_Obj *listObj = Tcl_NewObj();
	struct rd_kafka_metadata_broker *broker;

#define BROKER_STRING_FORMAT "%s:%d"

	for (i = 0 ; i < metadata->broker_cnt ; i++) {
		broker = &metadata->brokers[i];
		// figure out the size of the string we need
		int brokerStringLength = snprintf (NULL, 0, BROKER_STRING_FORMAT, broker->host, broker->port) + 1;
		char *brokerString = ckalloc (brokerStringLength);
		snprintf (brokerString, brokerStringLength, BROKER_STRING_FORMAT, broker->host, broker->port);

		if (Tcl_ListObjAppendElement (interp, listObj, Tcl_NewStringObj (brokerString, brokerStringLength)) == TCL_ERROR) {
			ckfree (brokerString);
			return TCL_ERROR;
		}
		ckfree (brokerString);
	}
	Tcl_SetObjResult (interp, listObj);
	return TCL_OK;
}

/*
 *----------------------------------------------------------------------
 *
 * kafkatcl_refresh_metadata --
 *
 *    fetch the metadata into our kafkatcl_handleClientData structure
 *
 * Results:
 *    a standard tcl result
 *
 *----------------------------------------------------------------------
 */
int
kafkatcl_refresh_metadata (kafkatcl_handleClientData *kh) {
	Tcl_Interp *interp = kh->interp;
	rd_kafka_t *rk = kh->rk;

	// destroy metadata if it exists
	if (kh->metadata != NULL) {
		rd_kafka_metadata_destroy (kh->metadata);
	}

	rd_kafka_resp_err_t err = rd_kafka_metadata (rk, 1, NULL, &kh->metadata, 5000);

	if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
		return kafkatcl_kafka_error_to_tcl (interp, err, "failed to acquire metadata");
	}

	return TCL_OK;
}

static void
metadata_print (const char *topic, const struct rd_kafka_metadata *metadata) {
	int i, j, k;

	printf("Metadata for %s (from broker %"PRId32": %s):\n",
	   topic ? : "all topics",
	   metadata->orig_broker_id,
	   metadata->orig_broker_name);


	/* Iterate brokers */
	printf(" %i brokers:\n", metadata->broker_cnt);
	for (i = 0 ; i < metadata->broker_cnt ; i++)
		printf("  broker %"PRId32" at %s:%i\n", metadata->brokers[i].id, metadata->brokers[i].host, metadata->brokers[i].port);

	/* Iterate topics */
	printf(" %i topics:\n", metadata->topic_cnt);
	for (i = 0 ; i < metadata->topic_cnt ; i++) {
		const struct rd_kafka_metadata_topic *t = &metadata->topics[i];

		printf("  topic \"%s\" with %i partitions:", t->topic, t->partition_cnt);
		if (t->err) {
			printf(" %s", rd_kafka_err2str(t->err));
			if (t->err == RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE)
				printf(" (try again)");
		}
		printf("\n");

		/* Iterate topic's partitions */
		for (j = 0 ; j < t->partition_cnt ; j++) {
			const struct rd_kafka_metadata_partition *p;
			p = &t->partitions[j];
			printf("    partition %"PRId32", " "leader %"PRId32", replicas: ", p->id, p->leader);

			/* Iterate partition's replicas */
			for (k = 0 ; k < p->replica_cnt ; k++) {
				printf("%s%"PRId32, k > 0 ? ",":"", p->replicas[k]);
			}

			/* Iterate partition's ISRs */
			printf(", isrs: ");

			for (k = 0 ; k < p->isr_cnt ; k++) {
				printf("%s%"PRId32, k > 0 ? ",":"", p->isrs[k]);
				if (p->err) {
					printf(", %s\n", rd_kafka_err2str(p->err));
				} else {
					printf("\n");
				}
			}
		}
	}
}

/*
 *----------------------------------------------------------------------
 *
 * kafkatcl_consume_callback_eventProc --
 *
 *    this routine is called by the Tcl event handler to process consume
 *    callbacks we have gotten from the Kafka cpp-driver
 *
 * Results:
 *    returns 1 to say we handled the event and the dispatcher can delete it
 *
 *----------------------------------------------------------------------
 */
int
kafkatcl_consume_callback_eventProc (Tcl_Event *tevPtr, int flags) {

	// we got called with a Tcl_Event pointer but really it's a pointer to
	// our kafkatcl_statsEvent structure that has the Tcl_Event and
	// some other stuff that we need.
	// Go get that.

	kafkatcl_consumeCallbackEvent *evPtr = (kafkatcl_consumeCallbackEvent *)tevPtr;
	kafkatcl_runningConsumer *krc = evPtr->krc;
	Tcl_Interp *interp = krc->kh->interp;

	Tcl_Obj *listObj = kafkatcl_message_to_tcl_list (interp, &evPtr->rkmessage);

	// even if this fails we still want the event taken off the queue
	// this function will do the background error thing if there is a tcl
	// error running the callback

	if (listObj != NULL) {
		kafkatcl_invoke_callback_with_argument (interp, krc->callbackObj, listObj);
		krc->pendingCallbackCount--;
	}

	// free the payload
	ckfree (evPtr->rkmessage.payload);

	// free the key if there is one
	if (evPtr->rkmessage.key != NULL) {
		ckfree (evPtr->rkmessage.key);
	}

	// tell the dispatcher we handled it.  0 would mean we didn't deal with
	// it and don't want it removed from the queue
	return 1;
}

/*
 *----------------------------------------------------------------------
 *
 * kafkatcl_consume_callback_queue_eventProc --
 *
 *    this routine is called by the Tcl event handler to process consume
 *    callbacks we have gotten for queues from the Kafka cpp-driver
 *
 * Results:
 *    returns 1 to say we handled the event and the dispatcher can delete it
 *
 *----------------------------------------------------------------------
 */
int
kafkatcl_consume_callback_queue_eventProc (Tcl_Event *tevPtr, int flags) {

	// we got called with a Tcl_Event pointer but really it's a pointer to
	// our kafkatcl_statsEvent structure that has the Tcl_Event and
	// some other stuff that we need.
	// Go get that.

	kafkatcl_consumeCallbackEvent *evPtr = (kafkatcl_consumeCallbackEvent *)tevPtr;
	kafkatcl_runningConsumer *krc = evPtr->krc;
	Tcl_Interp *interp = krc->kh->interp;

	Tcl_Obj *listObj = kafkatcl_message_to_tcl_list (interp, &evPtr->rkmessage);

	// even if this fails we still want the event taken off the queue
	// this function will do the background error thing if there is a tcl
	// error running the callback
	if (listObj != NULL) {
		// free the payload
		kafkatcl_invoke_callback_with_argument (interp, krc->callbackObj, listObj);
		krc->pendingCallbackCount--;
	}

	ckfree (evPtr->rkmessage.payload);

	// free the key if there is one
	if (evPtr->rkmessage.key != NULL) {
		ckfree (evPtr->rkmessage.key);
	}

	// tell the dispatcher we handled it.  0 would mean we didn't deal with
	// it and don't want it removed from the queue
	return 1;
}

/*
 *----------------------------------------------------------------------
 *
 * kafkatcl_consume_callback --
 *
 *    this routine is called by the kafka cpp-driver as a callback
 *    designated by a call to rd_kafka_consume_callback to specify
 *    a routine to be called when messages are available from a
 *    topic consumer or queue
 *
 * Results:
 *    an event is queued to the thread that set up the callback
 *
 *----------------------------------------------------------------------
 */

void
kafkatcl_consume_callback (rd_kafka_message_t *rkmessage, void *opaque) {
	kafkatcl_runningConsumer *krc = opaque;

	kafkatcl_consumeCallbackEvent *evPtr;

	evPtr = ckalloc (sizeof (kafkatcl_consumeCallbackEvent));

	evPtr->krc = krc;

	if (krc->kq == NULL) {
		evPtr->event.proc = kafkatcl_consume_callback_eventProc;
	} else {
		evPtr->event.proc = kafkatcl_consume_callback_queue_eventProc;
	}

	// structure copy
	evPtr->rkmessage = *rkmessage;

	// then allocate and copy the payload and possibly the key; we will free
	// all this in the event handler
	evPtr->rkmessage.payload = ckalloc (rkmessage->len);
	memcpy (evPtr->rkmessage.payload, rkmessage->payload, rkmessage->len);

	if (rkmessage->key != NULL) {
		evPtr->rkmessage.key = ckalloc (rkmessage->key_len);
		memcpy (evPtr->rkmessage.key, rkmessage->key, rkmessage->key_len);
	}

	Tcl_ThreadQueueEvent (krc->kh->threadId, (Tcl_Event *)evPtr, TCL_QUEUE_TAIL);
	return;
}

/*
 *----------------------------------------------------------------------
 *
 * kafkatcl_set_conf --
 *
 *    given an object client data and a topic config var and value,
 *		set the topic configuration property
 *
 *    returns an error if rd_kafka_conf_set returns an error
 *
 * Results:
 *    a standard tcl result
 *
 *----------------------------------------------------------------------
 */
int
kafkatcl_set_conf (kafkatcl_objectClientData *ko, char *name, char *value) {
	Tcl_Interp *interp = ko->interp;
	char errStr[256];

	rd_kafka_conf_res_t res = rd_kafka_conf_set (ko->conf, name, value, errStr, sizeof(errStr));

	if (res != RD_KAFKA_CONF_OK) {
		Tcl_SetObjResult (interp, Tcl_NewStringObj (errStr, -1));
		return TCL_ERROR;
	}
	return TCL_OK;
}

/*
 *----------------------------------------------------------------------
 *
 * kafkatcl_set_topic_conf --
 *
 *    given an object client data and a topic config var and value,
 *		set the topic configuration property
 *
 *    returns an error if rd_kafka_topic_conf_set returns an error
 *
 * Results:
 *    a standard tcl result
 *
 *----------------------------------------------------------------------
 */
int
kafkatcl_set_topic_conf (Tcl_Interp *interp, rd_kafka_topic_conf_t *topicConf, char *name, char *value) {
	char errStr[256];

	rd_kafka_conf_res_t res = rd_kafka_topic_conf_set (topicConf, name, value, errStr, sizeof(errStr));

	if (res != RD_KAFKA_CONF_OK) {
		Tcl_SetObjResult (interp, Tcl_NewStringObj (errStr, -1));
		return TCL_ERROR;
	}
	return TCL_OK;
}

/*
 *----------------------------------------------------------------------
 *
 * kafkatcl_handle_topic_conf --
 *
 *    given an object client data and a topic config var and value,
 *		set the topic configuration property
 *
 *    returns an error if rd_kafka_topic_conf_set returns an error
 *
 * Results:
 *    a standard tcl result
 *
 *----------------------------------------------------------------------
 */
int
kafkatcl_handle_topic_conf (Tcl_Interp *interp, rd_kafka_topic_conf_t *topicConf, int objc, Tcl_Obj *CONST objv[]) {
	if (objc % 2 != 0) {
		Tcl_WrongNumArgs (interp, 2, objv, "?name value ...?");
		return TCL_ERROR;
	}

	if (objc == 0) {
		return kafkatcl_topic_conf_to_list (interp, topicConf);
	}

	int i;
	int resultCode = TCL_OK;
	for (i = 0; i < objc; i += 2) {
		char *name = Tcl_GetString (objv[i]);
		char *value = Tcl_GetString (objv[i + 1]);

		resultCode = kafkatcl_set_topic_conf (interp, topicConf, name, value);

		if (resultCode == TCL_ERROR) {
			break;
		}
	}
	return TCL_OK;
}

/*
 *----------------------------------------------------------------------
 *
 * kafkatcl_partitioner_conf --
 *
 *    given an object client data and a topic config structure,
 *    parse the partitioner name and set the partitioner into
 *    the topic conf if it can be figured out
 *
 * Results:
 *    a standard tcl result
 *
 *----------------------------------------------------------------------
 */
int
kafkatcl_partitioner_conf (Tcl_Interp *interp, rd_kafka_topic_conf_t *topicConf, int objc, Tcl_Obj *CONST objv[]) {
	int suboptIndex;

	static CONST char *subOptions[] = {
		"random",
		"consistent",
		NULL
	};

	enum subOptions {
		SUBOPT_RANDOM_PARTITIONER,
		SUBOPT_CONSISTENT_PARTITIONER,
	};

	// argument must be one of the subOptions defined above
	if (Tcl_GetIndexFromObj (interp, objv[0], subOptions, "suboption",
		TCL_EXACT, &suboptIndex) != TCL_OK) {
		return TCL_ERROR;
	}

	switch ((enum subOptions) suboptIndex) {
		case SUBOPT_RANDOM_PARTITIONER: {
			rd_kafka_topic_conf_set_partitioner_cb (topicConf, rd_kafka_msg_partitioner_random);
			break;
		}

		case SUBOPT_CONSISTENT_PARTITIONER: {
			rd_kafka_topic_conf_set_partitioner_cb (topicConf, rd_kafka_msg_partitioner_consistent);
			break;
		}

	}
	return TCL_OK;
}

/*
 *----------------------------------------------------------------------
 *
 * kafkatcl_handle_topic_info --
 *
 *    given a pointer to a topic client data and an objc and objv for
 *    a topic producer or topic consumer object, handle the "info"
 *    suboption for that topic consumer or producer
 *
 * Results:
 *    a standard tcl result
 *
 *----------------------------------------------------------------------
 */
int
kafkatcl_handle_topic_info (Tcl_Interp *interp, kafkatcl_topicClientData *kt, int objc, Tcl_Obj *CONST objv[]) {
	int suboptIndex;

	if ((objc < 3) || (objc > 4)) {
		Tcl_WrongNumArgs (interp, 2, objv, "option");
		return TCL_ERROR;
	}

	static CONST char *subOptions[] = {
		"name",
		"partitions",
		"consistent_partition",
		NULL
	};

	enum subOptions {
		SUBOPT_NAME,
		SUBOPT_PARTITIONS,
		SUBOPT_CONSISTENT_PARTITION
	};

	// argument must be one of the subOptions defined above
	if (Tcl_GetIndexFromObj (interp, objv[2], subOptions, "suboption",
		TCL_EXACT, &suboptIndex) != TCL_OK) {
		return TCL_ERROR;
	}

	kafkatcl_handleClientData *kh = kt->kh;

	if (kh->metadata == NULL) {
		kafkatcl_refresh_metadata (kh);
	}

	switch ((enum subOptions) suboptIndex) {
		case SUBOPT_NAME: {
			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 3, objv, "");
				return TCL_ERROR;
			}

			Tcl_SetObjResult (interp, Tcl_NewStringObj (kt->topic, -1));
			break;
		}

		case SUBOPT_PARTITIONS: {
			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 3, objv, "");
				return TCL_ERROR;
			}

			return kafkatcl_meta_topic_partitions (kh, kt->topic);
		}

		case SUBOPT_CONSISTENT_PARTITION: {
			char *key = NULL;
			int keyLen = 0;
			int whichPartition;
			const struct rd_kafka_metadata_topic *t;

			if (objc != 4) {
				Tcl_WrongNumArgs (interp, 3, objv, "key");
				return TCL_ERROR;
			}

			if (kafkatcl_meta_find_topic_tcl_result (kh, kt->topic, &t) == TCL_ERROR) {
				return TCL_ERROR;
			}

			key = Tcl_GetStringFromObj (objv[3], &keyLen);

			if (t->partition_cnt == 0) {
				whichPartition = -1;
			} else {
				whichPartition = rd_kafka_msg_partitioner_consistent (kt->rkt, key, keyLen, t->partition_cnt, NULL, NULL);
			}

			Tcl_SetObjResult (interp, Tcl_NewIntObj (whichPartition));
			break;
		}
	}
	return TCL_OK;
}

/*
 *----------------------------------------------------------------------
 *
 * kafkatcl_consume_start --
 *
 *    given a pointer to a topic client data, a partition, an offset,
 *    and a possibly NULL callback routine, arrange to consume
 *    from the topic and partition at the specified offset,
 *    calling the callback routine if it is non-NULL
 *
 * Results:
 *    a standard tcl result
 *
 *----------------------------------------------------------------------
 */
int
kafkatcl_consume_start (kafkatcl_topicClientData *kt, int partition, int64_t offset, Tcl_Obj *callbackObj) {
	Tcl_Interp *interp = kt->kh->interp;
	rd_kafka_topic_t *rkt = kt->rkt;

	// tell librdkafka we want to start consuming this topic, partition,
	// and offset
	if (rd_kafka_consume_start (rkt, partition, offset) < 0) {
		return kafktcl_errno_to_tcl_error (interp);
	}


	//   lock down the callback object if there is one.
	//
	//   allocate and fill a running consumer structure.
	//   insert the structure onto the list of running consumers for the topic.

	if (callbackObj != NULL) {
		Tcl_IncrRefCount (callbackObj);
	}

	kafkatcl_runningConsumer *krc = ckalloc (sizeof (kafkatcl_runningConsumer));
	krc->kh = kt->kh;
	krc->kt = kt;
	krc->kq = NULL;
	krc->partition = partition;
	krc->callbackObj = callbackObj;
	krc->pendingCallbackCount = 0;

	KT_LIST_INSERT_HEAD (&kt->runningConsumers, krc, runningConsumerInstance);

	return TCL_OK;
}

/*
 *----------------------------------------------------------------------
 *
 * kafkatcl_set_queue_consumer --
 *
 *    given a pointer to a queue client data and a tcl object containing
 *    the callback function, set up a running consumer instance and
 *    point to it from the queue client data.
 *
 *    handle the reference counts and work properly whether or not one
 *    was already defined.
 *
 * Results:
 *    a standard tcl result
 *
 *----------------------------------------------------------------------
 */
int
kafkatcl_set_queue_consumer (kafkatcl_queueClientData *kq, Tcl_Obj *callbackObj) {
	kafkatcl_runningConsumer *krc;

	Tcl_IncrRefCount (callbackObj);

	krc = kq->krc;

	if (krc == NULL) {
		krc = ckalloc (sizeof (kafkatcl_runningConsumer));
	} else {
		Tcl_DecrRefCount (krc->callbackObj);
	}

	krc->kq = kq;
	krc->kh = kq->kh;
	krc->kt = NULL;
	krc->partition = 0;
	krc->pendingCallbackCount = 0;
	krc->callbackObj = callbackObj;

	kq->krc = krc;

	return TCL_OK;
}

/*
 *----------------------------------------------------------------------
 *
 * kafkatcl_consume_start_queue --
 *
 *    given a pointer to a topic client data, a partition, an offset,
 *    and a queue object, arrange for librdkafka to consume
 *    from the topic and partition at the specified offset,
 *    queueing received messages into the specified queue.
 *
 * Results:
 *    a standard tcl result
 *
 *----------------------------------------------------------------------
 */
int
kafkatcl_consume_start_queue (kafkatcl_topicClientData *kt, int partition, int64_t offset, kafkatcl_queueClientData *kq) {
	Tcl_Interp *interp = kt->kh->interp;
	rd_kafka_topic_t *rkt = kt->rkt;

	if (rd_kafka_consume_start_queue (rkt, partition, offset, kq->rkqu) < 0) {
		return kafktcl_errno_to_tcl_error (interp);
	}

	return TCL_OK;
}

/*
 *----------------------------------------------------------------------
 *
 * kafkatcl_consume_stop --
 *
 *    given a pointer to a topic client data and a partition number,
 *    stop consuming from that partition.
 *
 *    delete from the list of running consumers if it's in there.
 *
 * Results:
 *    a standard tcl result
 *
 *----------------------------------------------------------------------
 */
int
kafkatcl_consume_stop (kafkatcl_topicClientData *kt, int partition) {
	kafkatcl_runningConsumer *krc;
	Tcl_Interp *interp = kt->kh->interp;

	if (rd_kafka_consume_stop (kt->rkt, partition) < 0) {
		return kafktcl_errno_to_tcl_error (interp);
	}

	KT_LIST_FOREACH(krc, &kt->runningConsumers, runningConsumerInstance) {
		if (krc->partition == partition) {
			KT_LIST_REMOVE (krc, runningConsumerInstance);
			ckfree (krc);
			break;
		}
	}

	return TCL_OK;
}

/*
 *----------------------------------------------------------------------
 *
 * kafkatcl_consume_stop_all_partitions --
 *
 *    given a pointer to a topic client data and a partition, stop
 *    consuming.  delete from the list of running consumers if it's
 *    in there.
 *
 * Results:
 *    a standard tcl result
 *
 *----------------------------------------------------------------------
 */
void
kafkatcl_consume_stop_all_partitions (kafkatcl_topicClientData *kt) {
	kafkatcl_runningConsumer *krc;
	kafkatcl_runningConsumer *tmp;

	// for each running consumer (perhaps multiple partitions)
	KT_LIST_FOREACH_SAFE (krc, &kt->runningConsumers, runningConsumerInstance, tmp) {
		kafkatcl_consume_stop (kt, krc->partition);
		KT_LIST_REMOVE (krc, runningConsumerInstance);
		ckfree (krc);
	}
}


/*
 *----------------------------------------------------------------------
 *
 * kafkatcl_check_consumer_callbacks --
 *
 *    run through all the running consumers that have callbacks defined
 *    on partitions and topics and queues.
 *
 * Results:
 *    the numbers of messages consumed
 *
 *----------------------------------------------------------------------
 */
int
kafkatcl_check_consumer_callbacks (kafkatcl_objectClientData *ko) {
	kafkatcl_topicClientData *kt;
	kafkatcl_queueClientData *kq;
	int count = 0;
	int result;

	// for each defined topic consumer
	KT_LIST_FOREACH(kt, &ko->topicConsumers, topicConsumerInstance) {
		kafkatcl_runningConsumer *krc;

		// for each running consumer (perhaps multiple partitions)
		KT_LIST_FOREACH(krc, &kt->runningConsumers, runningConsumerInstance) {

			if (krc->callbackObj != NULL) {

				// get kafka to invoke our callback function for this
				result = rd_kafka_consume_callback (krc->kt->rkt, krc->partition, 0, kafkatcl_consume_callback, krc);
				if (result < 0) {
					// NB do something here
					// Tcl_BackgroundException (interp, TCL_ERROR);
				} else {
					krc->pendingCallbackCount += result;
					count += result;
				}
			}
		}
	}

	// for each queue see if there's a queue consumer and if so,
	// try to consume
	KT_LIST_FOREACH(kq, &ko->queueConsumers, queueConsumerInstance) {
		kafkatcl_runningConsumer *krc = kq->krc;

		if (krc == NULL) {
			continue;
		}

		result = rd_kafka_consume_callback_queue (kq->rkqu, 0, kafkatcl_consume_callback, krc);
		if (result < 0) {
			// NB do something here
			// Tcl_BackgroundException (interp, TCL_ERROR);
		} else {
			krc->pendingCallbackCount += result;
			count += result;
		}
	}

	return count;
}

/*
 *----------------------------------------------------------------------
 *
 * kafkatcl_topicConsumerObjectObjCmd --
 *
 *    dispatches the subcommands of a kafkatcl batch-handling command
 *
 * Results:
 *    stuff
 *
 *----------------------------------------------------------------------
 */
int
kafkatcl_topicConsumerObjectObjCmd(ClientData cData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    int         optIndex;
	kafkatcl_topicClientData *kt = (kafkatcl_topicClientData *)cData;
	rd_kafka_topic_t *rkt = kt->rkt;
	int resultCode = TCL_OK;

    static CONST char *options[] = {
        "consume",
        "consume_batch",
		"info",
        "consume_start",
        "consume_start_queue",
        "consume_stop",
        "delete",
        NULL
    };

    enum options {
		OPT_CONSUME,
		OPT_CONSUME_BATCH,
		OPT_INFO,
		OPT_CONSUME_START,
		OPT_CONSUME_START_QUEUE,
		OPT_CONSUME_STOP,
		OPT_DELETE
    };

    /* basic validation of command line arguments */
    if (objc < 2) {
        Tcl_WrongNumArgs (interp, 1, objv, "subcommand ?args?");
        return TCL_ERROR;
    }

    if (Tcl_GetIndexFromObj (interp, objv[1], options, "option", TCL_EXACT, &optIndex) != TCL_OK) {
		return TCL_ERROR;
    }

    switch ((enum options) optIndex) {
		case OPT_CONSUME: {
			int partition;
			int timeoutMS;

			if (objc != 5) {
				Tcl_WrongNumArgs (interp, 2, objv, "partition timeout array");
				return TCL_ERROR;
			}

			if (Tcl_GetIntFromObj (interp, objv[2], &partition) == TCL_ERROR) {
				resultCode = TCL_ERROR;
				break;
			}

			if (Tcl_GetIntFromObj (interp, objv[3], &timeoutMS) == TCL_ERROR) {
				resultCode = TCL_ERROR;
				break;
			}

			char *arrayName = Tcl_GetString (objv[4]);

			rd_kafka_message_t *rdm = rd_kafka_consume (rkt, partition, timeoutMS);

			if (rdm == NULL) {
				resultCode =  kafktcl_errno_to_tcl_error (interp);
				break;
			}

			resultCode = kafkatcl_message_to_tcl_array (interp, arrayName, rdm);

			if (resultCode == TCL_BREAK) {
				Tcl_SetObjResult (interp, Tcl_NewIntObj (0));
				return TCL_OK;
			}

			rd_kafka_message_destroy (rdm);

			Tcl_SetObjResult (interp, Tcl_NewIntObj (1));

			break;
		}

		case OPT_CONSUME_BATCH: {
			int partition;
			int timeoutMS;
			int count;

			if (objc != 7) {
				Tcl_WrongNumArgs (interp, 2, objv, "partition timeout count array code");
				return TCL_ERROR;
			}

			if (Tcl_GetIntFromObj (interp, objv[2], &partition) == TCL_ERROR) {
				resultCode = TCL_ERROR;
				break;
			}

			if (Tcl_GetIntFromObj (interp, objv[3], &timeoutMS) == TCL_ERROR) {
				resultCode = TCL_ERROR;
				break;
			}

			if (Tcl_GetIntFromObj (interp, objv[4], &count) == TCL_ERROR) {
				resultCode = TCL_ERROR;
				break;
			}

			char *arrayName = Tcl_GetString (objv[5]);

			Tcl_Obj *codeObj = objv[6];

			rd_kafka_message_t **rkMessages = ckalloc (sizeof (rd_kafka_message_t *) * count);

			int gotCount = rd_kafka_consume_batch (rkt, partition, timeoutMS, rkMessages, count);

			int i;

			for (i = 0; i < gotCount; i++) {
				resultCode = kafkatcl_message_to_tcl_array (interp, arrayName, rkMessages[i]);

				if (resultCode == TCL_BREAK) {
					resultCode = TCL_OK;
					goto done;
				}

				if (resultCode == TCL_ERROR) {
					break;
				}

				resultCode = Tcl_EvalObjEx (interp, codeObj,  0);

				if (resultCode == TCL_ERROR) {
					break;
				}

				rd_kafka_message_destroy (rkMessages[i]);
			}

			ckfree (rkMessages);

			if (resultCode != TCL_ERROR) {
			  done:
				Tcl_SetObjResult (interp, Tcl_NewIntObj (gotCount));
			}
			break;
		}

		case OPT_INFO: {
			return kafkatcl_handle_topic_info (interp, kt, objc, objv);
		}

		case OPT_CONSUME_START: {
			int64_t offset;
			int partition;
			Tcl_Obj *callbackObj = NULL;

			if ((objc < 4) || (objc > 5)) {
				Tcl_WrongNumArgs (interp, 2, objv, "partition offset ?callback?");
				return TCL_ERROR;
			}

			if (Tcl_GetIntFromObj (interp, objv[2], &partition) == TCL_ERROR) {
				resultCode = TCL_ERROR;
				break;
			}

			if (kafkatcl_parse_offset (interp, objv[3], &offset) != TCL_OK) {
				resultCode = TCL_ERROR;
				break;
			}

			if (objc == 5) {
				callbackObj = objv[4];
			}

			if (kafkatcl_consume_start (kt, partition, offset, callbackObj) == TCL_ERROR) {
				resultCode =  TCL_ERROR;
				break;
			}

			break;
		}

		case OPT_CONSUME_START_QUEUE: {
			int64_t offset;
			int partition;

			if (objc != 5) {
				Tcl_WrongNumArgs (interp, 2, objv, "partition offset queue");
				return TCL_ERROR;
			}

			if (Tcl_GetIntFromObj (interp, objv[2], &partition) == TCL_ERROR) {
				resultCode = TCL_ERROR;
				break;
			}

			if (kafkatcl_parse_offset (interp, objv[3], &offset) != TCL_OK) {
				resultCode = TCL_ERROR;
				break;
			}

			char *queueCommandName = Tcl_GetString (objv[4]);
			kafkatcl_queueClientData *qcd = kafkatcl_queue_command_to_queueClientData (interp, queueCommandName);
			if (qcd == NULL) {
				Tcl_SetObjResult (interp, Tcl_NewStringObj ("command name '", -1));
				Tcl_AppendResult (interp, queueCommandName, "' is not a kafkatcl queue object", NULL);
				resultCode = TCL_ERROR;
				break;
			}

			if (kafkatcl_consume_start_queue (kt, partition, offset, qcd) == TCL_ERROR) {
				resultCode =  TCL_ERROR;
				break;
			}

			break;
		}

		case OPT_CONSUME_STOP: {
			int partition;

			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "partition");
				return TCL_ERROR;
			}

			if (Tcl_GetIntFromObj (interp, objv[2], &partition) == TCL_ERROR) {
				resultCode = TCL_ERROR;
				break;
			}

			return kafkatcl_consume_stop (kt, partition);
		}

		case OPT_DELETE: {
			if (objc != 2) {
				Tcl_WrongNumArgs (interp, 2, objv, "");
				return TCL_ERROR;
			}

			if (Tcl_DeleteCommandFromToken (kt->kh->interp, kt->cmdToken) == TCL_ERROR) {
				resultCode = TCL_ERROR;
			}
			break;
		}
    }
    return resultCode;
}

/*
 *----------------------------------------------------------------------
 *
 * kafkatcl_topicProducerObjectObjCmd --
 *
 *    dispatches the subcommands of a kafkatcl batch-handling command
 *
 * Results:
 *    stuff
 *
 *----------------------------------------------------------------------
 */
int
kafkatcl_topicProducerObjectObjCmd(ClientData cData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    int         optIndex;
	kafkatcl_topicClientData *kt = (kafkatcl_topicClientData *)cData;
	rd_kafka_topic_t *rkt = kt->rkt;
	int resultCode = TCL_OK;

    static CONST char *options[] = {
        "produce",
        "produce_batch",
		"info",
        "delete",
        NULL
    };

    enum options {
		OPT_PRODUCE,
		OPT_PRODUCE_BATCH,
		OPT_INFO,
		OPT_DELETE
    };

    /* basic validation of command line arguments */
    if (objc < 2) {
        Tcl_WrongNumArgs (interp, 1, objv, "subcommand ?args?");
        return TCL_ERROR;
    }

    if (Tcl_GetIndexFromObj (interp, objv[1], options, "option", TCL_EXACT, &optIndex) != TCL_OK) {
		return TCL_ERROR;
    }

    switch ((enum options) optIndex) {
		case OPT_PRODUCE: {
			int partition;

			if (objc < 4 || objc > 5) {
				Tcl_WrongNumArgs (interp, 2, objv, "partition payload ?key?");
				return TCL_ERROR;
			}

			if (Tcl_GetIntFromObj (interp, objv[2], &partition) == TCL_ERROR) {
				resultCode = TCL_ERROR;
				break;
			}

			int payloadLength;
			unsigned char *payload = Tcl_GetByteArrayFromObj (objv[3], &payloadLength);

			const void *key = NULL;
			int keyLength = 0;

			if (objc == 5) {
				key = Tcl_GetByteArrayFromObj (objv[4], &keyLength);
			}

			if (rd_kafka_produce (rkt, partition, RD_KAFKA_MSG_F_COPY, payload, payloadLength, key, keyLength, kt) < 0) {
				resultCode =  kafktcl_errno_to_tcl_error (interp);
				break;
			}
			break;
		}

		case OPT_PRODUCE_BATCH: {
			int listObjc;
			Tcl_Obj **listObjv;
			int partition;

			if (objc != 4) {
				Tcl_WrongNumArgs (interp, 2, objv, "partition list-of-payload-key-lists");
				return TCL_ERROR;
			}

			if (Tcl_GetIntFromObj (interp, objv[2], &partition) == TCL_ERROR) {
				resultCode = TCL_ERROR;
				break;
			}

			if (Tcl_ListObjGetElements (interp, objv[3], &listObjc, &listObjv) == TCL_ERROR) {
				Tcl_AppendResult (interp, " while parsing list of partition-payload-key lists", NULL);
				resultCode = TCL_ERROR;
				break;
			}

			int i;

			if (listObjc == 0) {
				break;
			}

			rd_kafka_message_t *rkmessages = (rd_kafka_message_t *)ckalloc (sizeof(rd_kafka_message_t) * listObjc);

			for (i = 0; i < listObjc; i++) {
				int rowObjc;
				Tcl_Obj **rowObjv;

				if (Tcl_ListObjGetElements (interp, listObjv[i], &rowObjc, &rowObjv) == TCL_ERROR) {
					Tcl_AppendResult (interp, " while parsing list within partition-payload-key lists", NULL);
					resultCode = TCL_ERROR;
					goto batcherr;
				}

				if (rowObjc < 1 || rowObjc > 2) {
					Tcl_AppendResult (interp, " list within payload-key must contain payload and optional key", NULL);
					resultCode = TCL_ERROR;
					goto batcherr;
				}

				int payloadLength;
				unsigned char *payload = Tcl_GetByteArrayFromObj (rowObjv[0], &payloadLength);

				void *key = NULL;
				int keyLength = 0;

				rd_kafka_message_t *rk = &rkmessages[i];

				rk->payload = payload;
				rk->len = payloadLength;

				rk->key = key;
				rk->key_len = keyLength;
			}

			int nDone = rd_kafka_produce_batch (rkt, partition, RD_KAFKA_MSG_F_COPY, rkmessages, listObjc);
			ckfree(rkmessages);

			// NB dig through rkmessages looking for errors
			if (nDone != listObjc) {
				resultCode = TCL_ERROR;
			}

		  batcherr:
			break;
		}

		case OPT_INFO: {
			return kafkatcl_handle_topic_info (interp, kt, objc, objv);
		}

		case OPT_DELETE: {
			if (objc != 2) {
				Tcl_WrongNumArgs (interp, 2, objv, "");
				return TCL_ERROR;
			}

			if (Tcl_DeleteCommandFromToken (kt->kh->interp, kt->cmdToken) == TCL_ERROR) {
				resultCode = TCL_ERROR;
			}
			break;
		}
    }
    return resultCode;
}

/*
 *----------------------------------------------------------------------
 *
 * kafkatcl_createTopicObjectCommand --
 *
 *    given a kafkatcl_handleClientData pointer, an object name (or "#auto"),
 *    and a topic string, create a corresponding topic object command
 *
 * Results:
 *    A standard Tcl result
 *
 *----------------------------------------------------------------------
 */
int
kafkatcl_createTopicObjectCommand (kafkatcl_handleClientData *kh, char *cmdName, const char *topic)
{
	Tcl_Interp *interp = kh->interp;
	Tcl_ObjCmdProc *proc = NULL;

	// dup the topic conf that we pass to rd_kafka_topic_new because
	// rd_kafka_topic_new is documented as freeing the conf object
	// and we don't want to give up our copy
	rd_kafka_topic_conf_t *topicConf = rd_kafka_topic_conf_dup (kh->topicConf);
	rd_kafka_topic_t *rkt = rd_kafka_topic_new (kh->rk, topic, topicConf);

	if (rkt == NULL) {
		return kafktcl_errno_to_tcl_error (interp);
	}

	switch (kh->kafkaType) {
		case RD_KAFKA_PRODUCER:
			proc = kafkatcl_topicProducerObjectObjCmd;
			break;

		case RD_KAFKA_CONSUMER:
			proc = kafkatcl_topicConsumerObjectObjCmd;
			break;

		default:
			assert (0 == 1);
	}


	// allocate one of our kafka topic client data objects for Tcl and
	// configure it
	kafkatcl_topicClientData *kt = (kafkatcl_topicClientData *)ckalloc (sizeof (kafkatcl_topicClientData));

	kt->kafka_topic_magic = KAFKA_TOPIC_MAGIC;
	kt->rkt = rkt;
	kt->kh = kh;
	KT_LIST_INIT (&kt->runningConsumers);

	if (kh->kafkaType == RD_KAFKA_CONSUMER) {
		KT_LIST_INSERT_HEAD (&kh->ko->topicConsumers, kt, topicConsumerInstance);
	}

	kt->topic = ckalloc (strlen (topic) + 1);
	strcpy (kt->topic, topic);

#define TOPIC_STRING_FORMAT "kafka_topic%lu"
	// if cmdName is #auto, generate a unique name for the object
	int autoGeneratedName = 0;
	if (strcmp (cmdName, "#auto") == 0) {
		static unsigned long nextAutoCounter = 0;
		int baseNameLength = snprintf (NULL, 0, TOPIC_STRING_FORMAT, nextAutoCounter) + 1;
		cmdName = ckalloc (baseNameLength);
		snprintf (cmdName, baseNameLength, TOPIC_STRING_FORMAT, nextAutoCounter++);
		autoGeneratedName = 1;
	}

	// create a Tcl command to interface to the topic object
	kt->cmdToken = Tcl_CreateObjCommand (interp, cmdName, proc, kt, kafkatcl_topicObjectDelete);
	// set the full name to the command in the interpreter result
	Tcl_GetCommandFullName(interp, kt->cmdToken, Tcl_GetObjResult (interp));
	if (autoGeneratedName == 1) {
		ckfree(cmdName);
	}

	return TCL_OK;
}

/*
 *----------------------------------------------------------------------
 *
 * kafkatcl_queueObjectObjCmd --
 *
 *    dispatches the subcommands of a kafkatcl queue object
 *
 * Results:
 *    A standard Tcl result
 *
 *----------------------------------------------------------------------
 */
int
kafkatcl_queueObjectObjCmd(ClientData cData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    int         optIndex;
	kafkatcl_queueClientData *kq = (kafkatcl_queueClientData *)cData;
	rd_kafka_queue_t *rkqu = kq->rkqu;
	int resultCode = TCL_OK;

    static CONST char *options[] = {
        "consume",
        "consume_batch",
        "consume_callback",
        "delete",
        NULL
    };

    enum options {
		OPT_CONSUME_QUEUE,
		OPT_CONSUME_QUEUE_BATCH,
		OPT_CONSUME_CALLBACK,
		OPT_DELETE
    };

    /* basic validation of command line arguments */
    if (objc < 2) {
        Tcl_WrongNumArgs (interp, 1, objv, "subcommand ?args?");
        return TCL_ERROR;
    }

    if (Tcl_GetIndexFromObj (interp, objv[1], options, "option", TCL_EXACT, &optIndex) != TCL_OK) {
		return TCL_ERROR;
    }

    switch ((enum options) optIndex) {
		case OPT_CONSUME_QUEUE: {
			int timeoutMS;

			if (objc != 4) {
				Tcl_WrongNumArgs (interp, 2, objv, "timeout array");
				return TCL_ERROR;
			}

			if (Tcl_GetIntFromObj (interp, objv[2], &timeoutMS) == TCL_ERROR) {
				resultCode = TCL_ERROR;
				break;
			}

			char *arrayName = Tcl_GetString (objv[3]);

			rd_kafka_message_t *rdm = rd_kafka_consume_queue (rkqu, timeoutMS);

			if (rdm == NULL) {
				resultCode =  kafktcl_errno_to_tcl_error (interp);
				break;
			}

			resultCode = kafkatcl_message_to_tcl_array (interp, arrayName, rdm);
			rd_kafka_message_destroy (rdm);

			break;
		}

		// NB bears an awful lot in common with OPT_CONSUME_QUEUE elsewhere
		case OPT_CONSUME_QUEUE_BATCH: {
			int timeoutMS;
			int count;

			if (objc != 6) {
				Tcl_WrongNumArgs (interp, 2, objv, "timeout count array code");
				return TCL_ERROR;
			}

			if (Tcl_GetIntFromObj (interp, objv[2], &timeoutMS) == TCL_ERROR) {
				resultCode = TCL_ERROR;
				break;
			}

			if (Tcl_GetIntFromObj (interp, objv[3], &count) == TCL_ERROR) {
				resultCode = TCL_ERROR;
				break;
			}

			char *arrayName = Tcl_GetString (objv[4]);

			Tcl_Obj *codeObj = objv[5];

			rd_kafka_message_t **rkMessages = ckalloc (sizeof (rd_kafka_message_t *) * count);

			int gotCount = rd_kafka_consume_batch_queue (rkqu, timeoutMS, rkMessages, count);

			int i;

			for (i = 0; i < gotCount; i++) {
				resultCode = kafkatcl_message_to_tcl_array (interp, arrayName, rkMessages[i]);
				rd_kafka_message_destroy (rkMessages[i]);

				if (resultCode == TCL_ERROR) {
					break;
				}

				resultCode = Tcl_EvalObjEx (interp, codeObj,  0);

				if (resultCode == TCL_ERROR) {
					break;
				}

				if (resultCode == TCL_BREAK) {
					resultCode = TCL_OK;
					break;
				}
			}

			ckfree (rkMessages);

			if (resultCode != TCL_ERROR) {
				Tcl_SetObjResult (interp, Tcl_NewIntObj (gotCount));
			}

			break;
		}

		case OPT_CONSUME_CALLBACK: {
            if ((objc < 2) || (objc > 3)) {
                Tcl_WrongNumArgs (interp, 2, objv, "?callback?");
                return TCL_ERROR;
            }

            if (objc == 2) {
                if (kq->krc == NULL) {
                    break;
                }
                Tcl_SetObjResult (interp, kq->krc->callbackObj);
				break;
			}

			return kafkatcl_set_queue_consumer (kq, objv[2]);
		}

		case OPT_DELETE: {
			if (objc != 2) {
				Tcl_WrongNumArgs (interp, 2, objv, "");
				return TCL_ERROR;
			}

			if (Tcl_DeleteCommandFromToken (kq->kh->interp, kq->cmdToken) == TCL_ERROR) {
				resultCode = TCL_ERROR;
			}
			break;
		}
    }
    return resultCode;
}

/*
 *----------------------------------------------------------------------
 *
 * kafkatcl_add_brokers --
 *
 *    given a handle client data and a char* with some brokers,
 *    try to add the brokers
 *
 * Results:
 *    a standard tcl result
 *
 *----------------------------------------------------------------------
 */
int
kafkatcl_add_brokers (kafkatcl_handleClientData *kh, char *brokers) {
	Tcl_Interp *interp = kh->interp;

	if (rd_kafka_brokers_add (kh->rk, brokers) == 0) {
		Tcl_SetObjResult (interp, Tcl_NewStringObj ("No valid brokers specified", -1));
		return TCL_ERROR;
	}
	return TCL_OK;
}

/*
 *----------------------------------------------------------------------
 *
 * kafkatcl_handleObjectObjCmd --
 *
 *    dispatches the subcommands of a kafkatcl handle-handling command
 *
 * Results:
 *    stuff
 *
 *----------------------------------------------------------------------
 */
int
kafkatcl_handleObjectObjCmd(ClientData cData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    int         optIndex;
	kafkatcl_handleClientData *kh = (kafkatcl_handleClientData *)cData;
	rd_kafka_t *rk = kh->rk;
	int resultCode = TCL_OK;

    static CONST char *options[] = {
        "name",
        "new_topic",
		"log_level",
		"add_brokers",
		"create_queue",
		"output_queue_length",
		"meta",
		"info",
		"config",
		"partitioner",
        "delete",
        NULL
    };

    enum options {
        OPT_NAME,
		OPT_NEW_TOPIC,
		OPT_LOG_LEVEL,
		OPT_ADD_BROKERS,
        OPT_CREATE_QUEUE,
        OPT_OUTPUT_QUEUE_LENGTH,
		OPT_META,
		OPT_INFO,
		OPT_TOPIC_CONFIG,
		OPT_PARTITIONER,
		OPT_DELETE
    };

    /* basic validation of command line arguments */
    if (objc < 2) {
        Tcl_WrongNumArgs (interp, 1, objv, "subcommand ?args?");
        return TCL_ERROR;
    }

    if (Tcl_GetIndexFromObj (interp, objv[1], options, "option", TCL_EXACT, &optIndex) != TCL_OK) {
		return TCL_ERROR;
    }

    switch ((enum options) optIndex) {
		case OPT_NAME: {
			if (objc != 2) {
				Tcl_WrongNumArgs (interp, 2, objv, "");
				return TCL_ERROR;
			}

			Tcl_SetObjResult (interp, Tcl_NewStringObj (rd_kafka_name (rk), -1));
			break;
		}

		case OPT_NEW_TOPIC: {
			if (objc != 4) {
				Tcl_WrongNumArgs (interp, 2, objv, "cmdName topic");
				return TCL_ERROR;
			}

			char *cmdName = Tcl_GetString (objv[2]);
			char *topic = Tcl_GetString (objv[3]);

			resultCode = kafkatcl_createTopicObjectCommand (kh, cmdName, topic);
			break;
		}

		case OPT_LOG_LEVEL: {
			int kafkaLogLevel;

			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "level");
				return TCL_ERROR;
			}

			if (kafkatcl_obj_to_log_level (interp, objv[2], &kafkaLogLevel) == TCL_OK) {
				rd_kafka_set_log_level (rk, kafkaLogLevel);
			} else {
				return TCL_ERROR;
			}
			break;
		}

		case OPT_ADD_BROKERS: {
			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "brokerList");
				return TCL_ERROR;
			}

			char *brokers = Tcl_GetString (objv[2]);
			resultCode = kafkatcl_add_brokers (kh, brokers);
			break;
		}

		case OPT_CREATE_QUEUE: {
			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "command");
				return TCL_ERROR;
			}

			// allocate one of our kafka client data objects for Tcl and configure it
			kafkatcl_queueClientData *kq = (kafkatcl_queueClientData *)ckalloc (sizeof (kafkatcl_queueClientData));

			kq->kafka_queue_magic = KAFKA_QUEUE_MAGIC;
			kq->interp = interp;
			kq->rkqu = rd_kafka_queue_new  (rk);
			kq->kh = kh;
			kq->krc = NULL;

			KT_LIST_INSERT_HEAD (&kh->ko->queueConsumers, kq, queueConsumerInstance);

			char *cmdName = Tcl_GetString (objv[2]);

#define QUEUE_STRING_FORMAT "kafka_queue%lu"
			// if cmdName is #auto, generate a unique name for the object
			int autoGeneratedName = 0;
			if (strcmp (cmdName, "#auto") == 0) {
				static unsigned long nextAutoCounter = 0;
				int baseNameLength = snprintf (NULL, 0, QUEUE_STRING_FORMAT, nextAutoCounter) + 1;
				cmdName = ckalloc (baseNameLength);
				snprintf (cmdName, baseNameLength, QUEUE_STRING_FORMAT, nextAutoCounter++);
				autoGeneratedName = 1;
			}

			// create a Tcl command to interface to the handle object
			kq->cmdToken = Tcl_CreateObjCommand (interp, cmdName, kafkatcl_queueObjectObjCmd, kq, kafkatcl_queueObjectDelete);
			// set the full name to the command in the interpreter result
			Tcl_GetCommandFullName(interp, kq->cmdToken, Tcl_GetObjResult (interp));
			if (autoGeneratedName == 1) {
				ckfree(cmdName);
			}

			break;
		}

		case OPT_OUTPUT_QUEUE_LENGTH: {
			if (objc != 2) {
				Tcl_WrongNumArgs (interp, 2, objv, "");
				return TCL_ERROR;
			}

			Tcl_SetObjResult (interp, Tcl_NewIntObj (rd_kafka_outq_len (rk)));

		}

		case OPT_META: {
			int suboptIndex;

			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "refresh|print");
				return TCL_ERROR;
			}

			static CONST char *subOptions[] = {
				"refresh",
				"print",
				NULL
			};

			enum subOptions {
				SUBOPT_REFRESH,
				SUBOPT_PRINT,
			};

			// argument must be one of the subOptions defined above
			if (Tcl_GetIndexFromObj (interp, objv[2], subOptions, "suboption",
				TCL_EXACT, &suboptIndex) != TCL_OK) {
				return TCL_ERROR;
			}

			switch ((enum subOptions) suboptIndex) {
				case SUBOPT_REFRESH: {
					if (kafkatcl_refresh_metadata (kh) == TCL_ERROR) {
						return TCL_ERROR;
					}
					break;
				}

				case SUBOPT_PRINT: {
					if (kh->metadata == NULL) {
						if (kafkatcl_refresh_metadata (kh) == TCL_ERROR) {
							return TCL_ERROR;
						}
					}

					metadata_print (NULL, kh->metadata);
					break;
				}
			}
			break;
		}

		case OPT_INFO: {
			int suboptIndex;

			if (objc < 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "?topics?");
				return TCL_ERROR;
			}

			static CONST char *subOptions[] = {
				"topics",
				"brokers",
				"partitions",
				NULL
			};

			enum subOptions {
				SUBOPT_TOPICS,
				SUBOPT_BROKERS,
				SUBOPT_PARTITIONS,
			};

			// argument must be one of the subOptions defined above
			if (Tcl_GetIndexFromObj (interp, objv[2], subOptions, "suboption",
				TCL_EXACT, &suboptIndex) != TCL_OK) {
				return TCL_ERROR;
			}

			if (kh->metadata == NULL) {
				kafkatcl_refresh_metadata (kh);
			}

			switch ((enum subOptions) suboptIndex) {
				case SUBOPT_TOPICS: {
					if (objc != 3) {
						Tcl_WrongNumArgs (interp, 3, objv, "");
						return TCL_ERROR;
					}

					return kafkatcl_meta_topic_list (kh);
				}

				case SUBOPT_BROKERS: {
					if (objc != 3) {
						Tcl_WrongNumArgs (interp, 3, objv, "");
						return TCL_ERROR;
					}

					return kafkatcl_meta_broker_list (kh);
				}

				case SUBOPT_PARTITIONS: {
					if (objc != 4) {
						Tcl_WrongNumArgs (interp, 3, objv, "topic");
						return TCL_ERROR;
					}

					return kafkatcl_meta_topic_partitions (kh, Tcl_GetString (objv[3]));
				}
			}
			break;
		}

		case OPT_TOPIC_CONFIG: {
			resultCode = kafkatcl_handle_topic_conf (interp, kh->topicConf, objc - 2, &objv[2]);
			break;
		}

		case OPT_PARTITIONER: {
			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "consistent|random");
				return TCL_ERROR;
			}

			resultCode = kafkatcl_partitioner_conf (interp, kh->topicConf, objc - 2, &objv[2]);
			break;
		}


		case OPT_DELETE: {
			if (objc != 2) {
				Tcl_WrongNumArgs (interp, 2, objv, "");
				return TCL_ERROR;
			}

			if (Tcl_DeleteCommandFromToken (kh->interp, kh->cmdToken) == TCL_ERROR) {
				resultCode = TCL_ERROR;
			}
			break;
		}

    }
    return resultCode;
}

/*
 *----------------------------------------------------------------------
 *
 * kafkatcl_createHandleObjectCommand --
 *
 *    given a kafkatcl_objectClientData pointer, an object name (or "#auto"),
 *    and a handle type of producer or consumer, create a handle object
 *
 * Results:
 *    A standard Tcl result
 *
 *----------------------------------------------------------------------
 */
int
kafkatcl_createHandleObjectCommand (kafkatcl_objectClientData *ko, char *cmdName, rd_kafka_type_t kafkaType)
{
	char errStr[256];

	// allocate one of our kafka handle client data objects for Tcl and
	// configure it
	kafkatcl_handleClientData *kh = (kafkatcl_handleClientData *)ckalloc (sizeof (kafkatcl_handleClientData));
	Tcl_Interp *interp = ko->interp;

	// rd_kafka_new consumes its conf object so give it one because
	// we don't want to give ours up
	rd_kafka_conf_t *conf = rd_kafka_conf_dup (ko->conf);

	// create the handle
	rd_kafka_t *rk = rd_kafka_new (kafkaType, conf, errStr, sizeof(errStr));

	if (rk == NULL) {
		Tcl_SetObjResult (interp, Tcl_NewStringObj (errStr, -1));
		return TCL_ERROR;
	}

	kh->kafka_handle_magic = KAFKA_HANDLE_MAGIC;
	kh->interp = interp;
	kh->rk = rk;
	kh->ko = ko;
	kh->kafkaType = kafkaType;
	kh->threadId = Tcl_GetCurrentThread ();
	kh->metadata = NULL;
	kh->topicConf = rd_kafka_topic_conf_dup (ko->topicConf);

	Tcl_CreateEventSource (kafkatcl_EventSetupProc, kafkatcl_EventCheckProc, (ClientData) kh);

#define HANDLE_STRING_FORMAT "kafka_handle%lu"
	// if cmdName is #auto, generate a unique name for the object
	int autoGeneratedName = 0;
	if (strcmp (cmdName, "#auto") == 0) {
		static unsigned long nextAutoCounter = 0;
		int baseNameLength = snprintf (NULL, 0, HANDLE_STRING_FORMAT, nextAutoCounter) + 1;
		cmdName = ckalloc (baseNameLength);
		snprintf (cmdName, baseNameLength, HANDLE_STRING_FORMAT, nextAutoCounter++);
		autoGeneratedName = 1;
	}

	// create a Tcl command to interface to the handle object
	kh->cmdToken = Tcl_CreateObjCommand (interp, cmdName, kafkatcl_handleObjectObjCmd, kh, kafkatcl_handleObjectDelete);
	// set the full name to the command in the interpreter result
	Tcl_GetCommandFullName(interp, kh->cmdToken, Tcl_GetObjResult (interp));
	if (autoGeneratedName == 1) {
		ckfree(cmdName);
	}

	return TCL_OK;
}

/*
 *----------------------------------------------------------------------
 *
 * kafkatcl_kafkaObjectObjCmd --
 *
 *    dispatches the subcommands of a kafka object command
 *
 * Results:
 *    stuff
 *
 *----------------------------------------------------------------------
 */
int
kafkatcl_kafkaObjectObjCmd(ClientData cData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    int         optIndex;
	kafkatcl_objectClientData *ko = (kafkatcl_objectClientData *)cData;
	int resultCode = TCL_OK;

    static CONST char *options[] = {
        "config",
        "producer_creator",
        "consumer_creator",
		"topic_config",
		"partitioner",
        "delivery_report",
        "error_callback",
		"statistics_callback",
		"logger",
		"delete",
        NULL
    };

    enum options {
        OPT_CONFIG,
        OPT_PRODUCER_CREATOR,
        OPT_CONSUMER_CREATOR,
		OPT_TOPIC_CONFIG,
		OPT_PARTITIONER,
        OPT_DELIVERY_REPORT,
        OPT_SET_ERROR_CALLBACK,
        OPT_SET_STATISTICS_CALLBACK,
		OPT_LOGGER,
		OPT_DELETE
    };

    /* basic validation of command line arguments */
    if (objc < 2) {
        Tcl_WrongNumArgs (interp, 1, objv, "subcommand ?args?");
        return TCL_ERROR;
    }

    if (Tcl_GetIndexFromObj (interp, objv[1], options, "option", TCL_EXACT, &optIndex) != TCL_OK) {
		return TCL_ERROR;
    }

    switch ((enum options) optIndex) {
		case OPT_CONFIG: {
			if (objc % 2 != 0) {
				Tcl_WrongNumArgs (interp, 2, objv, "?name value ...?");
				return TCL_ERROR;
			}

			if (objc == 2) {
				resultCode = kafkatcl_conf_to_list (interp, ko->conf);
			} else {
				int i;

				for (i = 2; i < objc; i += 2) {
					char *name = Tcl_GetString (objv[i]);
					char *value = Tcl_GetString (objv[i + 1]);

					resultCode = kafkatcl_set_conf (ko, name, value);

					if (resultCode == TCL_ERROR) {
						break;
					}
				}
			}
			break;
		}

		case OPT_TOPIC_CONFIG: {
			resultCode = kafkatcl_handle_topic_conf (interp, ko->topicConf, objc - 2, &objv[2]);
			break;
		}

		case OPT_PARTITIONER: {
			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "consistent|random");
				return TCL_ERROR;
			}

			resultCode = kafkatcl_partitioner_conf (interp, ko->topicConf, objc - 2, &objv[2]);
			break;
		}

		case OPT_CONSUMER_CREATOR:
		case OPT_PRODUCER_CREATOR: {
			rd_kafka_type_t type;

			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "cmdName");
				return TCL_ERROR;
			}

			if (optIndex== OPT_CONSUMER_CREATOR) {
				type = RD_KAFKA_CONSUMER;
			} else {
				type = RD_KAFKA_PRODUCER;
			}

			char *cmdName = Tcl_GetString (objv[2]);
			resultCode = kafkatcl_createHandleObjectCommand (ko, cmdName, type);
			break;
		}

		case OPT_DELIVERY_REPORT: {
			int suboptIndex;

			if ((objc < 3) || (objc > 4)) {
				Tcl_WrongNumArgs (interp, 2, objv, "option ?args?");
				return TCL_ERROR;
			}

			static CONST char *subOptions[] = {
				"callback",
				"sample",
				"every",
				NULL
			};

			enum subOptions {
				SUBOPT_CALLBACK,
				SUBOPT_SAMPLE,
				SUBOPT_EVERY
			};

			// argument must be one of the subOptions defined above
			if (Tcl_GetIndexFromObj (interp, objv[2], subOptions, "suboption",
				TCL_EXACT, &suboptIndex) != TCL_OK) {
				return TCL_ERROR;
			}

			switch ((enum subOptions) suboptIndex) {
				case SUBOPT_CALLBACK: {
					if (objc != 4) {
						Tcl_WrongNumArgs (interp, 3, objv, "command");
						return TCL_ERROR;
					}

					if (ko->deliveryReportCallbackObj != NULL) {
						Tcl_DecrRefCount (ko->deliveryReportCallbackObj);
					}

					ko->deliveryReportCallbackObj = objv[3];
					Tcl_IncrRefCount (ko->deliveryReportCallbackObj);

					rd_kafka_conf_set_dr_msg_cb (ko->conf, kafkatcl_delivery_report_callback);
					break;
				}

				case SUBOPT_SAMPLE: {
					if (objc != 3) {
						Tcl_WrongNumArgs (interp, 3, objv, "");
						return TCL_ERROR;
					}
					ko->sampleDeliveryReport = 1;
					break;
				}

				case SUBOPT_EVERY: {
					if (objc > 4) {
						Tcl_WrongNumArgs (interp, 3, objv, "?count?");
						return TCL_ERROR;
					}

					if (objc == 3) {
						Tcl_SetObjResult (interp, Tcl_NewIntObj (ko->deliveryReportCountdown));
						break;
					}

					if (Tcl_GetIntFromObj (interp, objv[3], &ko->deliveryReportEvery) == TCL_ERROR) {
						resultCode = TCL_ERROR;
						break;
					}

					// if we've already been counting, reset the countdown
					// to the new "every" value
					if (ko->deliveryReportCountdown > 0) {
						ko->deliveryReportCountdown = ko->deliveryReportEvery;
					}

					break;
				}

			}

			break;
		}

		case OPT_SET_ERROR_CALLBACK: {
			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "command");
				return TCL_ERROR;
			}

			if (ko->errorCallbackObj != NULL) {
				Tcl_DecrRefCount (ko->errorCallbackObj);
			}

			ko->errorCallbackObj = objv[2];
			Tcl_IncrRefCount (ko->errorCallbackObj);

			rd_kafka_conf_set_error_cb (ko->conf, kafkatcl_error_callback);
			break;
		}

		case OPT_SET_STATISTICS_CALLBACK: {
			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "command");
				return TCL_ERROR;
			}

			if (ko->statisticsCallbackObj != NULL) {
				Tcl_DecrRefCount (ko->statisticsCallbackObj);
			}

			ko->statisticsCallbackObj = objv[2];
			Tcl_IncrRefCount (ko->statisticsCallbackObj);

			rd_kafka_conf_set_stats_cb (ko->conf, kafkatcl_stats_callback);
			break;
		}

		case OPT_LOGGER: {
			int         suboptIndex;

			if ((objc < 3) || (objc > 4)) {
				Tcl_WrongNumArgs (interp, 2, objv, "syslog|stderr|none|callback ?function?");
				return TCL_ERROR;
			}

			static CONST char *subOptions[] = {
				"syslog",
				"stderr",
				"none",
				"callback",
				NULL
			};

			enum subOptions {
				SUBOPT_SYSLOG,
				SUBOPT_STDERR,
				SUBOPT_NONE,
				SUBOPT_CALLBACK
			};

			// argument must be one of the subOptions defined above
			if (Tcl_GetIndexFromObj (interp, objv[2], subOptions, "suboption",
				TCL_EXACT, &suboptIndex) != TCL_OK) {
				return TCL_ERROR;
			}

			if (suboptIndex == SUBOPT_CALLBACK) {
				if (objc != 4) {
					Tcl_WrongNumArgs (interp, 2, objv, "callback function");
					return TCL_ERROR;

				}
			} else {
				if (objc != 3) {
					Tcl_WrongNumArgs (interp, 2, objv, Tcl_GetString (objv[3]));
					return TCL_ERROR;
				}
			}

			switch ((enum subOptions) suboptIndex) {
				case SUBOPT_SYSLOG: {
					// log to syslog by binding the kafka cpp-driver-supplied
					// syslog-logging routine
					rd_kafka_conf_set_log_cb (ko->conf, rd_kafka_log_syslog);
					break;
				}

				case SUBOPT_STDERR: {
					// log to stderr by binding the kafka cpp-driver-supplied
					// stderr-logging routine
					rd_kafka_conf_set_log_cb (ko->conf, rd_kafka_log_print);
					break;
				}

				case SUBOPT_NONE: {
					// suppress logging
					rd_kafka_conf_set_log_cb (ko->conf, NULL);
					break;
				}

				case SUBOPT_CALLBACK: {
					kafkatcl_loggingCallbackThreadId = Tcl_GetCurrentThread ();
					loggingInterp = interp;

					if (kafkatcl_loggingCallbackObj != NULL) {
						Tcl_DecrRefCount (kafkatcl_loggingCallbackObj);
					}

					kafkatcl_loggingCallbackObj = objv[3];
					Tcl_IncrRefCount (kafkatcl_loggingCallbackObj);

					rd_kafka_conf_set_log_cb (ko->conf, kafkatcl_logging_callback);
					break;
				}
			}
			break;
		}

		case OPT_DELETE: {
			if (objc != 2) {
				Tcl_WrongNumArgs (interp, 2, objv, "");
				return TCL_ERROR;
			}

			if (Tcl_DeleteCommandFromToken (ko->interp, ko->cmdToken) == TCL_ERROR) {
				resultCode = TCL_ERROR;
			}
			break;
		}
	}

    return resultCode;
}

/*
 *----------------------------------------------------------------------
 *
 * kafkatcl_kafkaObjCmd --
 *
 *      Create a kafka object...
 *
 *      kafka create my_kafka
 *      kafka create #auto
 *
 * The created object is invoked to do things with kafka queues
 *
 * Results:
 *      A standard Tcl result.
 *
 *
 *----------------------------------------------------------------------
 */

    /* ARGSUSED */
int
kafkatcl_kafkaObjCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    kafkatcl_objectClientData *ko;
    int                 optIndex;
    char               *cmdName;
    int                 autoGeneratedName;

    static CONST char *options[] = {
        "create",
        "version",
        NULL
    };

    enum options {
        OPT_CREATE,
		OPT_VERSION
    };

    // basic command line processing
    if (objc < 2) {
        Tcl_WrongNumArgs (interp, 1, objv, "subcommand ?args?");
        return TCL_ERROR;
    }

    // argument must be one of the subOptions defined above
    if (Tcl_GetIndexFromObj (interp, objv[1], options, "option",
        TCL_EXACT, &optIndex) != TCL_OK) {
        return TCL_ERROR;
    }

    switch ((enum options) optIndex) {
		case OPT_VERSION: {
			if (objc != 2) {
				Tcl_WrongNumArgs (interp, 2, objv, "");
				return TCL_ERROR;
			}
			Tcl_SetObjResult (interp, Tcl_NewStringObj (rd_kafka_version_str(), -1));
			return TCL_OK;
		}

		case OPT_CREATE: {
			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "command");
				return TCL_ERROR;
			}

			// allocate one of our kafka client data objects for Tcl and configure it
			ko = (kafkatcl_objectClientData *)ckalloc (sizeof (kafkatcl_objectClientData));

			ko->kafka_object_magic = KAFKA_OBJECT_MAGIC;
			ko->interp = interp;
			ko->conf = rd_kafka_conf_new ();
			ko->topicConf = rd_kafka_topic_conf_new ();
			rd_kafka_topic_conf_set_opaque (ko->topicConf, ko);

			ko->loggingCallbackObj = NULL;
			ko->deliveryReportCallbackObj = NULL;
			ko->errorCallbackObj = NULL;
			ko->statisticsCallbackObj = NULL;

			ko->sampleDeliveryReport = 0;
			ko->deliveryReportEvery = 1;
			ko->deliveryReportCountdown = 0;

			ko->threadId = Tcl_GetCurrentThread();

			// set the kafka conf opaque pointer so we can find
			// the corresponding kafkatcl_objectClientData structure
			rd_kafka_conf_set_opaque (ko->conf, ko);

			KT_LIST_INIT (&ko->topicConsumers);
			KT_LIST_INIT (&ko->queueConsumers);

			cmdName = Tcl_GetString (objv[2]);

			// if cmdName is #auto, generate a unique name for the object
			autoGeneratedName = 0;
			if (strcmp (cmdName, "#auto") == 0) {
				static unsigned long nextAutoCounter = 0;
				int    baseNameLength;

#define OBJECT_STRING_FORMAT "kafka_object%lu"
				baseNameLength = snprintf (NULL, 0, OBJECT_STRING_FORMAT, nextAutoCounter) + 1;
				cmdName = ckalloc (baseNameLength);
				snprintf (cmdName, baseNameLength, OBJECT_STRING_FORMAT, nextAutoCounter++);
				autoGeneratedName = 1;
			}

			// create a Tcl command to interface to kafka
			ko->cmdToken = Tcl_CreateObjCommand (interp, cmdName, kafkatcl_kafkaObjectObjCmd, ko, kafkatcl_kafkaObjectDelete);
			Tcl_SetObjResult (interp, Tcl_NewStringObj (cmdName, -1));
			if (autoGeneratedName == 1) {
				ckfree(cmdName);
			}
			break;
		}

	}

    return TCL_OK;
}

/* vim: set ts=4 sw=4 sts=4 noet : */



