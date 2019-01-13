---
layout: tutorials
title: Persistence with Queues
summary: Learn how to send and receive messages in a point-to-point fashion.
icon: I_dev_Persistent.svg
links:
    - label: QueuePublisher.c
      link: /blob/master/src/intro/QueuePublisher.c
    - label: QueueSubscriber.c
      link: /blob/master/src/intro/QueueSubscriber.c

---

This tutorial builds on the basic concepts introduced in the [publish/subscribe tutorial]({{ site.baseurl }}/publish-subscribe), and will show you how to send and receive persistent messages from a Solace message router queue in a point to point fashion.

## Assumptions

This tutorial assumes the following:

*   You are familiar with Solace [core concepts]({{ site.docs-core-concepts }}){:target="_top"}.
*   You have access to Solace messaging with the following configuration details:
    *   Connectivity information for a Solace message-VPN configured for guaranteed messaging support
    *   Enabled client username and password
    *   Client-profile enabled with guaranteed messaging permissions.

One simple way to get access to Solace messaging quickly is to create a messaging service in Solace Cloud [as outlined here]({{ site.links-solaceCloud-setup}}){:target="_top"}. You can find other ways to get access to Solace messaging below.

## Goals

The goal of this tutorial is to understand the following:

*   How to programmatically create a durable queue on the Solace message router
*   How to send a persistent message to a Solace queue
*   How to bind to this queue and receive a persistent message

{% include_relative assets/solaceMessaging.md %}
{% include_relative assets/solaceApi.md %}

## Connecting to the Solace message router

In order to send or receive messages, an application must connect a Solace session. The Solace session is the basis for all client communication with the Solace message router.

In the Solace messaging API for C (SolClient), a few distinct steps are required to create and connect a Solace session.

*   The API must be initialized
*   Appropriate asynchronous callbacks must be declared
*   A SolClient Context is needed to control application threading
*   The SolClient session must be created

### Initializing the CCSMP API

To initialize the SolClient API, you call the initialize method with arguments that control logging.

```cpp
/* solClient needs to be initialized before any other API calls. */
solClient_initialize ( SOLCLIENT_LOG_DEFAULT_FILTER, NULL );
```

This call must be made prior to making any other calls to the SolClient API. It allows the API to initialize internal state and buffer pools.

### SolClient Asynchronous Callbacks

The SolClient API is predominantly an asynchronous API designed for the highest speed and lowest latency. As such most events and notifications occur through callbacks. In order to get up and running, the following basic callbacks are required at a minimum.

```cpp
static int msgCount = 0;

solClient_rxMsgCallback_returnCode_t
sessionMessageReceiveCallback ( solClient_opaqueSession_pt opaqueSession_p, solClient_opaqueMsg_pt msg_p, void *user_p )
{
    printf ( "Received message:\n" );
    solClient_msg_dump ( msg_p, NULL, 0 );
    printf ( "\n" );

    msgCount++;
    return SOLCLIENT_CALLBACK_OK;
}

void
sessionEventCallback ( solClient_opaqueSession_pt opaqueSession_p,
                solClient_session_eventCallbackInfo_pt eventInfo_p, void *user_p )
{  
    printf("Session EventCallback() called:  %s\n", solClient_session_eventToString ( eventInfo_p->sessionEvent));
}
```

The messageReceiveCallback is invoked for each message received by the Session. In this sample, the message is printed to the screen.

The eventCallback is invoked for various significant session events like connection, disconnection, and other SolClient session events. In this sample, simply prints the events. See the [SolClient API documentation]({{ site.docs-api-ref }}){:target="_top"} and samples for details on the session events.

### Context Creation

As outlined in the core concepts, the context object is used to control threading that drives network I/O and message delivery and acts as containers for sessions. The easiest way to create a context is to use the context initializer with default thread creation.

```cpp
/* Context */
solClient_opaqueContext_pt context_p;
solClient_context_createFuncInfo_t contextFuncInfo = SOLCLIENT_CONTEXT_CREATEFUNC_INITIALIZER;

solClient_context_create ( SOLCLIENT_CONTEXT_PROPS_DEFAULT_WITH_CREATE_THREAD,
                           &context_p, &contextFuncInfo, sizeof ( contextFuncInfo ) );
```

### Session Creation

Finally a session is needed to actually connect to the Solace message router. This is accomplished by creating a properties array and connecting the session.

```cpp
/* Session */
solClient_opaqueSession_pt session_p;
solClient_session_createFuncInfo_t sessionFuncInfo = SOLCLIENT_SESSION_CREATEFUNC_INITIALIZER;

/* Session Properties */
const char     *sessionProps[50] = {0, };
int             propIndex = 0;
char *username,*password,*vpnname,*host;

/* Configure the Session function information. */
sessionFuncInfo.rxMsgInfo.callback_p = sessionMessageReceiveCallback;
sessionFuncInfo.rxMsgInfo.user_p = NULL;
sessionFuncInfo.eventInfo.callback_p = sessionEventCallback;
sessionFuncInfo.eventInfo.user_p = NULL;

/* Configure the Session properties. */
propIndex = 0;
host = argv[1];
vpnname = argv[2];
username = strsep(&vpnname,"@");
password = argv[3];

sessionProps[propIndex++] = SOLCLIENT_SESSION_PROP_HOST;
sessionProps[propIndex++] = host;

sessionProps[propIndex++] = SOLCLIENT_SESSION_PROP_VPN_NAME;
sessionProps[propIndex++] = vpnname;

sessionProps[propIndex++] = SOLCLIENT_SESSION_PROP_USERNAME;
sessionProps[propIndex++] = username;

sessionProps[propIndex++] = SOLCLIENT_SESSION_PROP_PASSWORD;
sessionProps[propIndex] = password;

/* Create the Session. */
solClient_session_create ( ( char ** ) sessionProps,
                           context_p,
                           &session_p, &sessionFuncInfo, sizeof ( sessionFuncInfo ) );

/* Connect the Session. */
solClient_session_connect ( session_p );
printf ( "Connected.\n" );
```

When creating the session, the factory method takes the session properties, the session pointer and information about the session callback functions. The API then creates the session within the supplied context and returns a reference in the session pointer. The final call to solClient_session_connect establishes the connection to the Solace message router which makes the session ready for use.

At this point your client is connected to the Solace message router. You can use SolAdmin to view the client connection and related details.

## Provisioning a Queue through the API

![message-router-queue]({{ site.baseurl }}/assets/images/message-router-queue.png)

The first requirement for guaranteed messaging using a Solace message router is to provision a guaranteed message endpoint. For this tutorial we will use a point-to-point queue. To learn more about different guaranteed message endpoints see [here]({{ site.docs-endpoints }}){:target="_top"}.

Durable endpoints are not auto created on Solace message routers. However there are two ways to provision them.

*   Using the management interface
*   Using the APIs

Using the Solace APIs to provision an endpoint can be a convenient way of getting started quickly without needing to become familiar with the management interface. This is why it is used in this tutorial. However it should be noted that the management interface provides more options to control the queue properties. So generally it becomes the preferred method over time.

Provisioning an endpoint through the API requires the “Guaranteed Endpoint Create” permission in the client-profile. You can confirm this is enabled by looking at the client profile in SolAdmin. If it is correctly set you will see the following:

![persistence-dotnet-1]({{ site.baseurl }}/assets/images/persistence-tutorial-image-3.png)

Provisioning the queue involves three steps.

1. Check if end point provisioning is supported.
   
   ```cpp
    /* Check if the endpoint provisioning is support */
    if ( !solClient_session_isCapable ( session_p, SOLCLIENT_SESSION_CAPABILITY_ENDPOINT_MANAGEMENT ) ) {

        printf ( "Endpoint management not supported on this appliance.\n" );
        return -1;
    }

   ```
2. Set the properties for your queue. This example sets the permission and the quota of the endpoint. More detail on the possible settings can be found in [developer documentation]({{ site.docs-api-ref }}){:target="_top"}.
   ```cpp
    /* Provision Properties */
    const char     *provProps[20] = {0, };
    int             provIndex;

    /* Configure the Provision properties */
    provIndex = 0;

    provProps[provIndex++] = SOLCLIENT_ENDPOINT_PROP_ID;
    provProps[provIndex++] = SOLCLIENT_ENDPOINT_PROP_QUEUE;

    provProps[provIndex++] = SOLCLIENT_ENDPOINT_PROP_NAME;
    provProps[provIndex++] = argv[5];

    provProps[provIndex++] = SOLCLIENT_ENDPOINT_PROP_PERMISSION;
    provProps[provIndex++] = SOLCLIENT_ENDPOINT_PERM_DELETE;

    provProps[provIndex++] = SOLCLIENT_ENDPOINT_PROP_QUOTA_MB;
    provProps[provIndex++] = "100";
   ```
3. Proceed to provision the queue.

```cpp
    /* Queue Network Name to be used with "solClient_session_endpointProvision()" */
    char            qNN[80];

    /* Try to provision the Queue. Ignore if already exists */
    solClient_session_endpointProvision ( ( char ** ) provProps,
                                          session_p,
                                          SOLCLIENT_PROVISION_FLAGS_WAITFORCONFIRM|
                                          SOLCLIENT_PROVISION_FLAGS_IGNORE_EXIST_ERRORS,
                                          NULL, qNN, sizeof ( qNN ) );
```


## Sending a message to a queue

Now it is time to send a message to the queue.

![sending-message-to-queue]({{ site.baseurl }}/assets/images/sending-message-to-queue.png)

To send a message, you must create a message and a queue destination. This tutorial will send a Solace binary message with contents "Hello world!". Then you must send the message to the Solace message router.


```cpp
     /* Message */
    solClient_opaqueMsg_pt msg_p = NULL;
    solClient_destination_t destination;
    const char *text_p = "Hello world!";
   
    /* Allocate a message. */
    solClient_msg_alloc ( &msg_p );

    /* Set the delivery mode for the message. */
    solClient_msg_setDeliveryMode ( msg_p, SOLCLIENT_DELIVERY_MODE_PERSISTENT );

    /* Set the destination. */
    destination.destType = SOLCLIENT_QUEUE_DESTINATION;
    destination.dest = argv[5];
    solClient_msg_setDestination ( msg_p, &destination, sizeof ( destination ) );

    /* Add some content to the message. */
    solClient_msg_setBinaryAttachment ( msg_p, text_p, ( solClient_uint32_t ) strlen ( (char *)text_p ) );

    /* Send the message. */
    printf ( "About to send message '%s' to queue '%s'...\n", (char *)text_p, argv[5] );
    solClient_session_sendMsg ( session_p, msg_p );
    printf ( "Message sent.\n" );

    /* Free the message. */
    solClient_msg_free ( &msg_p );

```

The message is transferred to the Solace message router asynchronously, but if all goes well, it will be waiting for your consumer on the queue. 

## Receiving a message from a queue

Now it is time to receive the messages sent to your queue.

![receiving-message-from-queue]({{ site.baseurl }}/assets/images/receiving-message-from-queue.png)

You still need to connect a session just as you did with the publisher. With a connected session, you then need to bind to the Solace message router queue with a flow receiver. Flow receivers allow applications to receive messages from a Solace guaranteed message flow. Flows encapsulate all of the acknowledgement behaviour required for guaranteed messaging. Conveniently flow receivers have the same interface as message consumers but flows also require some additional properties on creation.

A flow requires properties. At its most basic, the flow properties require the endpoint (our newly provisioned or existing queue) and an ack mode. In this example you’ll use the client ack mode where the application will explicitly acknowledge each message.

Flows are created from Solace session objects just as direct message consumers are.

Notice `flowMessageReceiveCallback` and `flowEventCallback` callbacks that are passed in when creating a flow. These callbacks will be invoked when a message arrives to the endpoint (the queue) or a flow events occurs.

You must start your flow so it can begin receiving messages.

```cpp

/*****************************************************************************
 * sessionEventCallback
 *
 * The event callback function is mandatory for session creation.
 *****************************************************************************/
void
sessionEventCallback ( solClient_opaqueSession_pt opaqueSession_p,
                solClient_session_eventCallbackInfo_pt eventInfo_p, void *user_p )
{
}

/*****************************************************************************
 * flowEventCallback
 *
 * The event callback function is mandatory for flow creation.
 *****************************************************************************/
static void
flowEventCallback ( solClient_opaqueFlow_pt opaqueFlow_p, solClient_flow_eventCallbackInfo_pt eventInfo_p, void *user_p )
{
}

/*************************************************************************
 * Create a Flow
*************************************************************************/
/* Flow Properties */
const char     *flowProps[20] = (0, };

/* Flow */
solClient_opaqueFlow_pt flow_p;
solClient_flow_createFuncInfo_t flowFuncInfo = SOLCLIENT_FLOW_CREATEFUNC_INITIALIZER;

/* Configure the Flow function information */
flowFuncInfo.rxMsgInfo.callback_p = flowMessageReceiveCallback;
flowFuncInfo.eventInfo.callback_p = flowEventCallback;

/* Configure the Flow properties */
propIndex = 0;

flowProps[propIndex++] = SOLCLIENT_FLOW_PROP_BIND_BLOCKING;
flowProps[propIndex++] = SOLCLIENT_PROP_DISABLE_VAL;

flowProps[propIndex++] = SOLCLIENT_FLOW_PROP_BIND_ENTITY_ID;
flowProps[propIndex++] = SOLCLIENT_FLOW_PROP_BIND_ENTITY_QUEUE;

flowProps[propIndex++] = SOLCLIENT_FLOW_PROP_ACKMODE;
flowProps[propIndex++] = SOLCLIENT_FLOW_PROP_ACKMODE_CLIENT;

flowProps[propIndex++] = SOLCLIENT_FLOW_PROP_BIND_NAME;
flowProps[propIndex++] = argv[5];

solClient_session_createFlow ( ( char ** ) flowProps,
                                session_p,
                                &flow_p, &flowFuncInfo, sizeof ( flowFuncInfo ) );
```

Both flow properties and endpoint properties are explained in more detail in the [developer documentation]({{ site.docs-api-ref }}){:target="_top"}.

Same as direct messaging, all messages will be received thru the invocation of `sessionEventCallback`.

## Summarizing

Combining the example source code shown above results in the following source code files:

<ul>
{% for item in page.links %}
<li><a href="{{ site.repository }}{{ item.link }}" target="_blank">{{ item.label }}</a></li>
{% endfor %}
</ul>

## Building

Building these examples is simple. 

#### For linux and mac
All you need is to execute the compile script in the `build` folder. 

linux
```
build$ ./build_intro_linux_xxx.sh
```
mac
```
build$ ./build_intro_mac_xxx.sh
```

#### For windows
You can either build the examples from DOS prompt or from Visual Studio IDE.  
To build from DOS prompt, you must first launch the appropriate Visual Studio Command Prompt and then run the batch file
```
c:\solace-sample-c\build>build_intro_win_xxx.bat
```

Referencing the downloaded SolClient library include and lib file is required. For more advanced build control, consider adapting the makefile found in the "Intro" directory of the SolClient package. The above samples very closely mirror the samples found there.

If you start the `QueuePublisher` with the required arguments of your Solace messaging, if will publish the message to the specified queue. In the example below, a message is published to a queue `q1`
```
bin$ . /setenv.sh
bin$ ./QueuePublisher <msg_backbone_ip:port> <message-vpn> <client-username> <password> <queue>
QueuePublisher initializing...
Connected.
About to send message 'Hello world!' to queue 'q1'...
Message sent.
Acknowledgement received!
Exiting.
```

You can next run the `QueueSubscriber` with the same queue `q1` and if the message has been successfully delivered and queued, the queued message will be consumed and printed out.

```
bin$ ./QueueSubscriber <msg_backbone_ip:port> <message-vpn> <client-username> <password> <queue>
Connected.
Waiting for messages......
Received message:
Destination:                            Queue 'q1'
Class Of Service:                       COS_1
DeliveryMode:                           PERSISTENT
Message Id:                             1
Binary Attachment:                      len=12
  48 65 6c 6c 6f 20 77 6f  72 6c 64 21                  Hello wo   rld!

Acknowledging message Id: 1.
Exiting.
```

You have now successfully connected a client, sent persistent messages to a queue and received and acknowledged them.

If you have any issues sending and receiving a message, check the [Solace community]({{ site.links-community }}){:target="_top"} for answers to common issues.
