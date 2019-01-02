
/** @example ex/dtoPubSub.c 
 */

/*
 * This sample demonstrates:
 *  - Publishing a message using Deliver-To-One (DTO).
 *  - Subscribing to a Topic using DTO override to receive all messages.
 *
 * Sample Requirements:
 *  - A Solace appliance running SolOS-TR
 *
 *
 * In this sample, three Sessions to a SolOS-TR appliance are created:
 *  Session 1 - Publish messages to a Topic with the DTO flag set.
 *            - Subscribe to the Topic with DTO override set.
 *  Session 2 - Subscribe to the Topic.
 *  Session 3 - Subscribe to the Topic.
 *
 * All Sessions subscribe to the same Topic. Therefore, with the DTO flag set
 * on messages being published, the appliance delivers messages to Session 2 
 * and Session 3 in a round robin manner. In addition to delivering the 
 * message to either Session 2 or Session 3, the appliance delivers all 
 * messages to Session 1.  
 *
 * Note: Session 1 is not part of the round robin to receive DTO messages
 * because its subscription uses DTO-override.
 *
 * Copyright 2009-2018 Solace Corporation. All rights reserved.
 */


/*****************************************************************************
 *  For Windows builds, os.h should always be included first to ensure that
 *  _WIN32_WINNT is defined before winsock2.h or windows.h get included.
 *****************************************************************************/
#include "os.h"
#include "solclient/solClient.h"
#include "solclient/solClientMsg.h"
#include "common.h"

/*****************************************************************************
 * publishDtoMessage
 *
 * Publishes an empty message to the topic MY_SAMPLE_TOPIC. An empty message
 * is used in this case because we only care about where it gets delivered, not
 * what the contents are. The Deliver-To-One flag is set on the message before
 * it is sent.
 *****************************************************************************/
static void
publishDtoMessage ( solClient_opaqueContext_pt opaqueContext_p, void *session_p )
{
    solClient_returnCode_t rc = SOLCLIENT_OK;
    solClient_opaqueMsg_pt msg_p = NULL;
    solClient_destination_t destination;

    solClient_log ( SOLCLIENT_LOG_DEBUG, "About to publish\n" );

    /* Allocate memory for the message that is to be sent. */
    if ( ( rc = solClient_msg_alloc ( &msg_p ) ) != SOLCLIENT_OK ) {
        common_handleError ( rc, "solClient_msg_alloc()" );
        return;
    }

    /* Set the message delivery mode. */
    if ( ( rc = solClient_msg_setDeliveryMode ( msg_p, SOLCLIENT_DELIVERY_MODE_DIRECT ) ) != SOLCLIENT_OK ) {
        common_handleError ( rc, "solClient_msg_setDeliveryMode()" );
        goto freeMessage;
    }

    /* Set the destination. */
    destination.destType = SOLCLIENT_TOPIC_DESTINATION;
    destination.dest = COMMON_MY_SAMPLE_TOPIC;
    if ( ( rc = solClient_msg_setDestination ( msg_p, &destination, sizeof ( destination ) ) ) != SOLCLIENT_OK ) {
        common_handleError ( rc, "solClient_msg_setDestination()" );
        goto freeMessage;
    }

    /*************************************************************************
     * Enable Deliver-To-One (DTO)
     ************************************************************************/
    solClient_msg_setDeliverToOne ( msg_p, 1 );

    /* Send the message. */
    if ( ( rc = solClient_session_sendMsg ( session_p, msg_p ) ) != SOLCLIENT_OK ) {
        common_handleError ( rc, "solClient_session_sendMsg()" );
        goto freeMessage;
    }

  freeMessage:
    if ( ( rc = solClient_msg_free ( &msg_p ) ) != SOLCLIENT_OK ) {
        common_handleError ( rc, "solClient_msg_free()" );
    }
}


/*****************************************************************************
 * main
 * 
 * The entry point to the application.
 *****************************************************************************/
int
main ( int argc, char *argv[] )
{
    solClient_returnCode_t rc = SOLCLIENT_OK;

    /* Command Options */
    struct commonOptions commandOpts;

    /* Context */
    solClient_opaqueContext_pt context_p;
    solClient_context_createFuncInfo_t contextFuncInfo = SOLCLIENT_CONTEXT_CREATEFUNC_INITIALIZER;

    /* Sessions */
    solClient_opaqueSession_pt session_p[3];
    int             sessionIndex;

    /* 
     * User pointers can be used when creating a Session to allow applications
     * to have access to additional information from within its callback
     * functions. The user pointer can refer to anything (void*), but in this
     * sample, a simple char* is used to give each Session a name that
     * will be used by the message receive callback to indicate which Session 
     * received the message.
     *
     * Note: It is important that the data the user pointer refers to
     * remains valid for the duration of the Context. This is a simple sample, 
     * therefore, the stack can be used, but the heap might be required  
     * for more complex applications.
     */
    char           *user_p[3] = { "DTO Override Session", "DTO Session 1", "DTO Session 2" };

    /* Timer callback for publish. */
    solClient_uint32_t interMessageDelay = 1000;
    solClient_context_timerId_t timerId;



    printf ( "\ndtoPubSub.c (Copyright 2009-2018 Solace Corporation. All rights reserved.)\n" );

    /* Intialize Control-C handling. */
    initSigHandler (  );

    /*************************************************************************
     * Parse command options
     *************************************************************************/
    common_initCommandOptions(&commandOpts, 
                               ( USER_PARAM_MASK ),    /* required parameters */
                               ( HOST_PARAM_MASK |
                                PASS_PARAM_MASK |
                                LOG_LEVEL_MASK |
                                USE_GSS_MASK |
                                ZIP_LEVEL_MASK));                       /* optional parameters */
    if ( common_parseCommandOptions ( argc, argv, &commandOpts, NULL ) == 0 ) {
        exit (1);
    }

    /*************************************************************************
     * Iinitialize the API and setup logging level
     *************************************************************************/

    /* solClient needs to be initialized before any other API calls. */
    if ( ( rc = solClient_initialize ( SOLCLIENT_LOG_DEFAULT_FILTER, NULL ) ) != SOLCLIENT_OK ) {
        common_handleError ( rc, "solClient_initialize()" );
        goto notInitialized;
    }

    common_printCCSMPversion (  );

    /* 
     * Standard logging levels can be set independently for the API and the
     * application. In this case, the ALL category is used to set the log level 
     * for both at the same time.
     */
    solClient_log_setFilterLevel ( SOLCLIENT_LOG_CATEGORY_ALL, commandOpts.logLevel );

    /*************************************************************************
     * Create a Context
     *************************************************************************/

    solClient_log ( SOLCLIENT_LOG_INFO, "Creating solClient context" );

    /* 
     * Create a Context, and specify that the Context thread should be created 
     * automatically instead of having the application create its own
     * Context thread.
     */
    if ( ( rc = solClient_context_create ( SOLCLIENT_CONTEXT_PROPS_DEFAULT_WITH_CREATE_THREAD,
                                           &context_p, &contextFuncInfo, sizeof ( contextFuncInfo ) ) ) != SOLCLIENT_OK ) {
        common_handleError ( rc, "solClient_context_create()" );
        goto cleanup;
    }

    /*************************************************************************
     * Create and connect three Sessions
     *************************************************************************/

    solClient_log ( SOLCLIENT_LOG_INFO, "Creating solClient sessions." );

    /* 
     * Create three Sessions; they are identical except for that they are given
     * different names to differentiate them.
     */
    for ( sessionIndex = 0; sessionIndex < 3; ++sessionIndex ) {

        /* 
         * createAndConnectSession is a common function used in these samples.
         * It is a wrapper for solClient_session_create() that applies some 
         * common properties to the Session, some of which are based on the 
         * command options. The wrapper also connects the Session.
         */
        if ( ( rc = common_createAndConnectSession ( context_p,
                                                     &session_p[sessionIndex],
                                                     common_messageReceiveCallback,
                                                     common_eventCallback,
                                                     user_p[sessionIndex], &commandOpts ) ) != SOLCLIENT_OK ) {
            common_handleError ( rc, "common_createAndConnectSession()" );
            goto cleanup;
        }
    }

    /*************************************************************************
     * Subscribe
     *************************************************************************/

    /* Session 1: Subscription with DTO override enabled. */
    if ( ( rc = solClient_session_topicSubscribeExt ( session_p[0],
                                                      SOLCLIENT_SUBSCRIBE_FLAGS_WAITFORCONFIRM |
                                                      SOLCLIENT_SUBSCRIBE_FLAGS_RX_ALL_DELIVER_TO_ONE,
                                                      COMMON_MY_SAMPLE_TOPIC ) ) != SOLCLIENT_OK ) {
        common_handleError ( rc, "solClient_session_topicSubscribe()" );
        goto sessionConnected;
    }

    /* Session 2: Regular subscription. */
    if ( ( rc = solClient_session_topicSubscribeExt ( session_p[1],
                                                      SOLCLIENT_SUBSCRIBE_FLAGS_WAITFORCONFIRM,
                                                      COMMON_MY_SAMPLE_TOPIC ) ) != SOLCLIENT_OK ) {
        common_handleError ( rc, "solClient_session_topicSubscribe()" );
        goto sessionConnected;
    }

    /* Session 3: Regular subscription. */
    if ( ( rc = solClient_session_topicSubscribeExt ( session_p[2],
                                                      SOLCLIENT_SUBSCRIBE_FLAGS_WAITFORCONFIRM,
                                                      COMMON_MY_SAMPLE_TOPIC ) ) != SOLCLIENT_OK ) {
        common_handleError ( rc, "solClient_session_topicSubscribe()" );
        goto sessionConnected;
    }

    /*************************************************************************
     * Publish using a Context timer
     *************************************************************************/

    if ( ( rc = solClient_context_startTimer ( context_p,
                                               SOLCLIENT_CONTEXT_TIMER_REPEAT,
                                               interMessageDelay,
                                               publishDtoMessage, ( void * ) session_p[0], &timerId ) ) != SOLCLIENT_OK ) {
        common_handleError ( rc, "solClient_context_startTimer()" );
        goto sessionConnected;
    }

    /*************************************************************************
     * Wait for CTRL-C
     *************************************************************************/

    printf ( "Sending and receiving, Ctrl-C to stop...\n" );
    semWait ( &ctlCSem );
    printf ( "Got Ctrl-C, cleaning up.\n" );

    /*************************************************************************
     * Cleanup
     *************************************************************************/
  sessionConnected:
    /* Disconnect the Session. */
    for ( sessionIndex = 0; sessionIndex < 3; ++sessionIndex ) {
        if ( ( rc = solClient_session_disconnect ( session_p[sessionIndex] ) ) != SOLCLIENT_OK ) {
            common_handleError ( rc, "solClient_session_disconnect()" );
        }
    }

  cleanup:
    /* Cleanup solClient. */
    if ( ( rc = solClient_cleanup (  ) ) != SOLCLIENT_OK ) {
        common_handleError ( rc, "solClient_cleanup()" );
    }

  notInitialized:
    return 0;

}
