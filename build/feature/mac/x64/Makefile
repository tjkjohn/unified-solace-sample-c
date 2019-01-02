ifeq ($(findstring Rules.mk, $(wildcard *.mk)), Rules.mk)
   include Rules.mk
endif

CXX:=gcc -g

ifeq ($(CCSMPHOME),)
	CCSMPHOME:=../../../..
endif

INCDIRS:=-I$(CCSMPHOME) \
	 -I$(CCSMPHOME)/inc \
	 -I.

SIXTY_FOUR_COMPAT:=
OS:=mac
ARCH:=x64
LINUXLIBDIR=$(CCSMPHOME)/lib/$(OS)/$(ARCH)
LIBDIRS:=-L$(CCSMPHOME)/lib -L$(LINUXLIBDIR)
LLSYS:=$(SIXTY_FOUR_COMPAT)
VPATH:=$(CCSMPHOME)/src/feature
OUTPUTDIR:=$(CCSMPHOME)/bin/feature/$(OS)/$(ARCH)
COMPILEFLAG:= $(COMPILEFLAG) $(INCDIRS) $(ARCHFLAGS) -DPROVIDE_LOG_UTILITIES -g
LINKFLAGS:= $(LIBDIRS) -lsolclient $(LLSYS)

$(shell mkdir -p $(OUTPUTDIR))

%.o:	%.c
	$(CXX) $(COMPILEFLAG)  $(SIXTY_FOUR_COMPAT) -c $< -o $(OUTPUTDIR)/$@

EXECS:= directPubSub perfADPub perfADSub perfTest \
        messageSelectorsOnQueue dtoPubSub asyncCacheRequest syncCacheRequest sempGetOverMb topicToQueueMapping \
        messageTTLAndDeadMessageQueue topicDispatch eventMonitor adPubAck simpleFlowToQueue \
        simpleFlowToTopic subscribeOnBehalfOfClient queueProvision redirectLogs sdtPubSubMsgDep sdtPubSubMsgIndep \
        messageReplay noLocalPubSub flowControlQueue simpleBrowserFlow cutThroughFlowToQueue replication \
        activeFlowIndication secureSession RRGuaranteedRequester RRGuaranteedReplier RRDirectRequester RRDirectReplier transactions

all: $(EXECS)

clean:
	 rm -rf $(OUTPUTDIR)/*

install:
	cp $(CCSMPHOME)/lib/$(OS)/$(ARCH)/*.* $(OUTPUTDIR)/  	
	rm $(OUTPUTDIR)/*.a  
	rm $(OUTPUTDIR)/*.o  

directPubSub : directPubSub.o $(DEPENDS)
	$(CXX) -o $(OUTPUTDIR)/$@ $(OUTPUTDIR)/$^ $(LINKFLAGS)

perfADPub : perfADPub.o $(DEPENDS)
	$(CXX) -o $(OUTPUTDIR)/$@ $(OUTPUTDIR)/$^ $(LINKFLAGS)

perfADSub : perfADSub.o $(DEPENDS)
	$(CXX) -o $(OUTPUTDIR)/$@ $(OUTPUTDIR)/$^ $(LINKFLAGS)

perfTest : perfTest.o $(DEPENDS)
	$(CXX) -o $(OUTPUTDIR)/$@ $(OUTPUTDIR)/$^ $(LINKFLAGS)

messageSelectorsOnQueue : messageSelectorsOnQueue.o $(DEPENDS)
	$(CXX) -o $(OUTPUTDIR)/$@ $(OUTPUTDIR)/$^ $(LINKFLAGS)

dtoPubSub : dtoPubSub.o $(DEPENDS)
	$(CXX) -o $(OUTPUTDIR)/$@ $(OUTPUTDIR)/$^ $(LINKFLAGS)

asyncCacheRequest : asyncCacheRequest.o $(DEPENDS)
	$(CXX) -o $(OUTPUTDIR)/$@ $(OUTPUTDIR)/$^ $(LINKFLAGS)

syncCacheRequest : syncCacheRequest.o $(DEPENDS)
	$(CXX) -o $(OUTPUTDIR)/$@ $(OUTPUTDIR)/$^ $(LINKFLAGS)

sempGetOverMb : sempGetOverMb.o $(DEPENDS)
	$(CXX) -o $(OUTPUTDIR)/$@ $(OUTPUTDIR)/$^ $(LINKFLAGS)

topicToQueueMapping : topicToQueueMapping.o $(DEPENDS)
	$(CXX) -o $(OUTPUTDIR)/$@ $(OUTPUTDIR)/$^ $(LINKFLAGS)

messageTTLAndDeadMessageQueue : messageTTLAndDeadMessageQueue.o $(DEPENDS)
	$(CXX) -o $(OUTPUTDIR)/$@ $(OUTPUTDIR)/$^ $(LINKFLAGS)

topicDispatch : topicDispatch.o $(DEPENDS)
	$(CXX) -o $(OUTPUTDIR)/$@ $(OUTPUTDIR)/$^ $(LINKFLAGS)

eventMonitor : eventMonitor.o $(DEPENDS)
	$(CXX) -o $(OUTPUTDIR)/$@ $(OUTPUTDIR)/$^ $(LINKFLAGS)

adPubAck : adPubAck.o $(DEPENDS)
	$(CXX) -o $(OUTPUTDIR)/$@ $(OUTPUTDIR)/$^ $(LINKFLAGS)

simpleFlowToQueue : simpleFlowToQueue.o $(DEPENDS)
	$(CXX) -o $(OUTPUTDIR)/$@ $(OUTPUTDIR)/$^ $(LINKFLAGS)

simpleFlowToTopic : simpleFlowToTopic.o $(DEPENDS)
	$(CXX) -o $(OUTPUTDIR)/$@ $(OUTPUTDIR)/$^ $(LINKFLAGS)

subscribeOnBehalfOfClient : subscribeOnBehalfOfClient.o $(DEPENDS)
	$(CXX) -o $(OUTPUTDIR)/$@ $(OUTPUTDIR)/$^ $(LINKFLAGS)

queueProvision : queueProvision.o $(DEPENDS)
	$(CXX) -o $(OUTPUTDIR)/$@ $(OUTPUTDIR)/$^ $(LINKFLAGS)

redirectLogs : redirectLogs.o $(DEPENDS)
	$(CXX) -o $(OUTPUTDIR)/$@ $(OUTPUTDIR)/$^ $(LINKFLAGS)

sdtPubSubMsgDep : sdtPubSubMsgDep.o $(DEPENDS)
	$(CXX) -o $(OUTPUTDIR)/$@ $(OUTPUTDIR)/$^ $(LINKFLAGS)

sdtPubSubMsgIndep : sdtPubSubMsgIndep.o $(DEPENDS)
	$(CXX) -o $(OUTPUTDIR)/$@ $(OUTPUTDIR)/$^ $(LINKFLAGS)

messageReplay : messageReplay.o $(DEPENDS)
	$(CXX) -o $(OUTPUTDIR)/$@ $(OUTPUTDIR)/$^ $(LINKFLAGS)

noLocalPubSub : noLocalPubSub.o $(DEPENDS)
	$(CXX) -o $(OUTPUTDIR)/$@ $(OUTPUTDIR)/$^ $(LINKFLAGS)

flowControlQueue : flowControlQueue.o $(DEPENDS)
	$(CXX) -o $(OUTPUTDIR)/$@ $(OUTPUTDIR)/$^ $(LINKFLAGS)

simpleBrowserFlow : simpleBrowserFlow.o $(DEPENDS)
	$(CXX) -o $(OUTPUTDIR)/$@ $(OUTPUTDIR)/$^ $(LINKFLAGS)

cutThroughFlowToQueue : cutThroughFlowToQueue.o $(DEPENDS)
	$(CXX) -o $(OUTPUTDIR)/$@ $(OUTPUTDIR)/$^ $(LINKFLAGS)

replication : replication.o $(DEPENDS)
	$(CXX) -o $(OUTPUTDIR)/$@ $(OUTPUTDIR)/$^ $(LINKFLAGS)

activeFlowIndication : activeFlowIndication.o $(DEPENDS)
	$(CXX) -o $(OUTPUTDIR)/$@ $(OUTPUTDIR)/$^ $(LINKFLAGS)

secureSession : secureSession.o $(DEPENDS)
	$(CXX) -o $(OUTPUTDIR)/$@ $(OUTPUTDIR)/$^ $(LINKFLAGS)

RRGuaranteedRequester : RRGuaranteedRequester.o $(DEPENDS)
	$(CXX) -o $(OUTPUTDIR)/$@ $(OUTPUTDIR)/$^ $(LINKFLAGS)

RRGuaranteedReplier : RRGuaranteedReplier.o  $(DEPENDS)
	$(CXX) -o $(OUTPUTDIR)/$@ $(OUTPUTDIR)/$^ $(LINKFLAGS)

RRDirectRequester : RRDirectRequester.o $(DEPENDS)
	$(CXX) -o $(OUTPUTDIR)/$@ $(OUTPUTDIR)/$^ $(LINKFLAGS)

RRDirectReplier : RRDirectReplier.o  $(DEPENDS)
	$(CXX) -o $(OUTPUTDIR)/$@ $(OUTPUTDIR)/$^ $(LINKFLAGS)

transactions : transactions.o  $(DEPENDS)
	$(CXX) -o $(OUTPUTDIR)/$@ $(OUTPUTDIR)/$^ $(LINKFLAGS)
