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
	Tcl_Obj *deliveryReportMessageCallbackObj;
	Tcl_Obj *deliveryReportCallbackObj;
	Tcl_Obj *errorCallbackObj;
	Tcl_Obj *statisticsCallbackObj;
} kafkatcl_objectClientData;

typedef struct kafkatcl_handleClientData
{
    int kafka_handle_magic;
    Tcl_Interp *interp;
    rd_kafka_t *rk;
	kafkatcl_objectClientData *ko;
    Tcl_Command cmdToken;
	rd_kafka_type_t kafkaType;
	Tcl_ThreadId threadId;
} kafkatcl_handleClientData;

typedef struct kafkatcl_topicClientData
{
    int kafka_topic_magic;
	rd_kafka_topic_t *rkt;
	kafkatcl_handleClientData *kh;
	Tcl_Command cmdToken;
	Tcl_Obj *consumeCallbackObj;
} kafkatcl_topicClientData;

typedef struct kafkatcl_queueClientData
{
    int kafka_queue_magic;
	rd_kafka_queue_t *rkqu;
	kafkatcl_handleClientData *kh;
	Tcl_Command cmdToken;
} kafkatcl_queueClientData;

typedef struct kafkatcl_deliveryReportEvent
{
    Tcl_Event event;
    Tcl_Interp *interp;
	rd_kafka_t *rk;
	void *payload;
	size_t len;
	rd_kafka_resp_err_t err;
} kafkatcl_deliveryReportEvent;

typedef struct kafkatcl_deliveryReportMessageEvent
{
    Tcl_Event event;
    Tcl_Interp *interp;
	rd_kafka_t *rk;
	const rd_kafka_message_t *rkmessage;
} kafkatcl_deliveryReportMessageEvent;

typedef struct kafkatcl_errorEvent
{
    Tcl_Event event;
    Tcl_Interp *interp;
	rd_kafka_t *rk;
	int err;
	const char *reason;
} kafkatcl_errorEvent;


typedef struct kafkatcl_loggingEvent
{
    Tcl_Event event;
    Tcl_Interp *interp;
	const rd_kafka_t *rk;
	int level;
	char *fac;
	char *buf;
} kafkatcl_loggingEvent;

typedef struct kafkatcl_statsEvent
{
    Tcl_Event event;
    Tcl_Interp *interp;
	rd_kafka_t *rk;
	char *json;
	size_t json_len;
} kafkatcl_statsEvent;

/* vim: set ts=4 sw=4 sts=4 noet : */
