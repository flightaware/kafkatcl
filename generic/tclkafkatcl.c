/*
 * kafkatcl_Init and kafkatcl_SafeInit
 *
 * Copyright (C) 2015 FlightAware
 *
 * Freely redistributable under the Berkeley copyright.  See license.terms
 * for details.
 */

#include <tcl.h>
#include "kafkatcl.h"

#undef TCL_STORAGE_CLASS
#define TCL_STORAGE_CLASS DLLEXPORT


/*
 *----------------------------------------------------------------------
 *
 * Kafka_Init --
 *
 *	Initialize the kafkatcl extension.  The string "kafkatcl"
 *      in the function name must match the PACKAGE declaration at the top of
 *	configure.in.
 *
 * Results:
 *	A standard Tcl result
 *
 * Side effects:
 *	One new command "kafkatcl" is added to the Tcl interpreter.
 *
 *----------------------------------------------------------------------
 */

EXTERN int
Kafka_Init(Tcl_Interp *interp)
{
    Tcl_Namespace *namespace;
    /*
     * This may work with 8.0, but we are using strictly stubs here,
     * which requires 8.1.
     */
    if (Tcl_InitStubs(interp, "8.1", 0) == NULL) {
		return TCL_ERROR;
    }

    if (Tcl_PkgRequire(interp, "Tcl", "8.1", 0) == NULL) {
		return TCL_ERROR;
    }

    if (Tcl_PkgProvide(interp, PACKAGE_NAME, PACKAGE_VERSION) != TCL_OK) {
		return TCL_ERROR;
    }

    namespace = Tcl_CreateNamespace (interp, "::kafka", NULL, NULL);

    /* Create the create command  */
    Tcl_CreateObjCommand(interp, "::kafka::kafka", (Tcl_ObjCmdProc *) kafkatcl_kafkaObjCmd, (ClientData)NULL, (Tcl_CmdDeleteProc *)NULL);

    Tcl_Export (interp, namespace, "*", 0);

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * Kafka_SafeInit --
 *
 *	Initialize the kafkatcl in a safe interpreter.
 *
 * Results:
 *	A standard Tcl result
 *
 * Side effects:
 *	One new command "kafkatcl" is added to the Tcl interpreter.
 *
 *----------------------------------------------------------------------
 */

EXTERN int
Kafka_SafeInit(Tcl_Interp *interp)
{
    /*
     * can this work safely?  I don't know...
     */
    return TCL_ERROR;
}

/* vim: set ts=4 sw=4 sts=4 noet : */
