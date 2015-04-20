/*
 *
 * Include file for kafkatcl package
 *
 * Copyright (C) 2015 by FlightAware, All Rights Reserved
 *
 * Freely redistributable under the Berkeley copyright, see license.terms
 * for details.
 */

#include <tcl.h>
#include <limits.h>
#include <string.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <syslog.h>
#include <librdkafka/rdkafka.h>

#define KAFKA_OBJECT_MAGIC 96451241
#define KAFKA_HANDLE_MAGIC 10758317
#define KAFKA_TOPIC_MAGIC 71077345
#define KAFKA_QUEUE_MAGIC 13377331

extern int
kafkatcl_kafkaObjCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objvp[]);

typedef struct kafkatcl_objectClientData
{
    int kafka_object_magic;
    Tcl_Interp *interp;
	rd_kafka_conf_t *conf;
	rd_kafka_topic_conf_t *topicConf;
    Tcl_Command cmdToken;
	Tcl_ThreadId threadId;
	Tcl_Obj *loggingCallbackObj;
} kafkatcl_objectClientData;

typedef struct kafkatcl_handleClientData
{
    int kafka_handle_magic;
    Tcl_Interp *interp;
    rd_kafka_t *rk;
	kafkatcl_objectClientData *ko;
    Tcl_Command cmdToken;
} kafkatcl_handleClientData;

typedef struct kafkatcl_topicClientData
{
    int kafka_topic_magic;
	rd_kafka_topic_t *rkt;
	kafkatcl_handleClientData *kh;
	Tcl_Command cmdToken;
} kafkatcl_topicClientData;

typedef struct kafkatcl_queueClientData
{
    int kafka_queue_magic;
	rd_kafka_queue_t *rkqu;
	kafkatcl_handleClientData *kh;
	Tcl_Command cmdToken;
} kafkatcl_queueClientData;


typedef struct kafkatcl_loggingEvent
{
    Tcl_Event event;
    Tcl_Interp *interp;
	// NB some kind of event pointer
} kafkatcl_loggingEvent;

/* vim: set ts=4 sw=4 sts=4 noet : */
