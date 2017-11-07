/* -*- mode: c; tab-width: 4; indent-tabs-mode: t -*- */

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

/* KT_LIST_* - bidirectionally linked list routines from BSD.
 * See LICENSE file for copyright information.
 */

#define	KT_LIST_HEAD(name, type)						\
struct name {								\
	struct type *lh_first;	/* first element */			\
}

#define	KT_LIST_HEAD_INITIALIZER(head)					\
	{ NULL }

#define	KT_LIST_ENTRY(type)						\
struct {								\
	struct type *le_next;	/* next element */			\
	struct type **le_prev;	/* address of previous next element */	\
}

/*
 * bidirectionally linked list functions, from BSD
 */

#define	KT_LIST_EMPTY(head)	((head)->lh_first == NULL)

#define	KT_LIST_FIRST(head)	((head)->lh_first)

#define	KT_LIST_FOREACH(var, head, field)					\
	for ((var) = KT_LIST_FIRST((head));				\
	    (var);							\
	    (var) = KT_LIST_NEXT((var), field))

#define	KT_LIST_FOREACH_SAFE(var, head, field, tvar)			\
	for ((var) = KT_LIST_FIRST((head));				\
	    (var) && ((tvar) = KT_LIST_NEXT((var), field), 1);		\
	    (var) = (tvar))

#define	KT_LIST_INIT(head) do {						\
	KT_LIST_FIRST((head)) = NULL;					\
} while (0)

#define	KT_LIST_INSERT_HEAD(head, elm, field) do {				\
	if ((KT_LIST_NEXT((elm), field) = KT_LIST_FIRST((head))) != NULL)	\
		KT_LIST_FIRST((head))->field.le_prev = &KT_LIST_NEXT((elm), field);\
	KT_LIST_FIRST((head)) = (elm);					\
	(elm)->field.le_prev = &KT_LIST_FIRST((head));			\
} while (0)

#define	KT_LIST_NEXT(elm, field)	((elm)->field.le_next)

#define	KT_LIST_REMOVE(elm, field) do {					\
	if (KT_LIST_NEXT((elm), field) != NULL)				\
		KT_LIST_NEXT((elm), field)->field.le_prev = 		\
		    (elm)->field.le_prev;				\
	*(elm)->field.le_prev = KT_LIST_NEXT((elm), field);		\
} while (0)

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
	Tcl_Obj *deliveryReportCallbackObj;
	Tcl_Obj *errorCallbackObj;
	Tcl_Obj *statisticsCallbackObj;

	int sampleDeliveryReport;			// if 1, call back on next produced msg
	int deliveryReportEvery;			// call back one out of this many
	int deliveryReportCountdown;		// counter for callback
	KT_LIST_HEAD(topicConsumers, kafkatcl_topicClientData) topicConsumers;
	KT_LIST_HEAD(queueConsumers, kafkatcl_queueClientData) queueConsumers;
} kafkatcl_objectClientData;

typedef struct kafkatcl_handleClientData
{
    int kafka_handle_magic;
    Tcl_Interp *interp;
    rd_kafka_t *rk;
	rd_kafka_topic_conf_t *topicConf;
	kafkatcl_objectClientData *ko;
    Tcl_Command cmdToken;
	rd_kafka_type_t kafkaType;
	Tcl_ThreadId threadId;
	const struct rd_kafka_metadata *metadata;
	int subscriber;
	Tcl_Obj *subscriberCallback;
} kafkatcl_handleClientData;

typedef struct kafkatcl_topicClientData
{
    int kafka_topic_magic;
	rd_kafka_topic_t *rkt;
	kafkatcl_handleClientData *kh;
	Tcl_Command cmdToken;
	char *topic;
	KT_LIST_ENTRY(kafkatcl_topicClientData) topicConsumerInstance;
	KT_LIST_HEAD(runningConsumers, kafkatcl_runningConsumer) runningConsumers;
} kafkatcl_topicClientData;

typedef struct kafkatcl_queueClientData
{
    int kafka_queue_magic;
	Tcl_Interp *interp;
	rd_kafka_queue_t *rkqu;
	kafkatcl_handleClientData *kh;
	Tcl_Command cmdToken;
	struct kafkatcl_runningConsumer *krc;
	KT_LIST_ENTRY(kafkatcl_queueClientData) queueConsumerInstance;
} kafkatcl_queueClientData;

typedef struct kafkatcl_deliveryReportEvent
{
    Tcl_Event event;
	kafkatcl_objectClientData *ko;
	rd_kafka_message_t rkmessage;
} kafkatcl_deliveryReportEvent;

typedef struct kafkatcl_errorEvent
{
    Tcl_Event event;
	kafkatcl_objectClientData *ko;
	int err;
	char *reason;
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
	kafkatcl_objectClientData *ko;
	rd_kafka_t *rk;
	char *json;
	size_t jsonLen;
} kafkatcl_statsEvent;

typedef struct kafkatcl_runningConsumer {
	kafkatcl_handleClientData *kh;
	kafkatcl_topicClientData *kt;
	kafkatcl_queueClientData *kq;
	int partition;
	Tcl_Obj *callbackObj;
	KT_LIST_ENTRY(kafkatcl_runningConsumer) runningConsumerInstance;
} kafkatcl_runningConsumer;

typedef struct kafkatcl_consumeCallbackEvent
{
    Tcl_Event event;
	kafkatcl_runningConsumer *krc;
	rd_kafka_message_t rkmessage;
} kafkatcl_consumeCallbackEvent;


/* vim: set ts=4 sw=4 sts=4 noet : */
