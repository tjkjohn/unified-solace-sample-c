ifeq ($(findstring Rules.mk, $(wildcard *.mk)), Rules.mk)
   include Rules.mk
endif

CXX:=gcc -g

ifeq ($(CCSMPHOME),)
	CCSMPHOME:=../../..
endif

INCDIRS:=-I$(CCSMPHOME) \
	 -I$(CCSMPHOME)/inc \
	 -I.

SIXTY_FOUR_COMPAT:=
OS:=linux
ARCH:=x64
LINUXLIBDIR=$(CCSMPHOME)/lib/$(OS)/$(ARCH)
LIBDIRS:=-L$(CCSMPHOME)/lib -L$(LINUXLIBDIR)
LLSYS:=$(SIXTY_FOUR_COMPAT)
VPATH:=$(CCSMPHOME)/src/intro
OUTPUTDIR:=$(CCSMPHOME)/bin/$(OS)/$(ARCH)
COMPILEFLAG:= $(COMPILEFLAG) $(INCDIRS) $(ARCHFLAGS) -DPROVIDE_LOG_UTILITIES -g
LINKFLAGS:= $(LIBDIRS) -lsolclient $(LLSYS)

$(shell mkdir -p $(OUTPUTDIR))

%.o:	%.c
	$(CXX) $(COMPILEFLAG)  $(SIXTY_FOUR_COMPAT) -c $< -o $(OUTPUTDIR)/$@

INTRO:= HelloWorldPub HelloWorldSub HelloWorldQueuePub HelloWorldQueueSub HelloWorldWebPub HelloWorldWebSub

all: $(INTRO)

intro: $(INTRO)

clean:
	 rm -rf $(OUTPUTDIR)/*

install:
	cp $(CCSMPHOME)/lib/$(OS)/$(ARCH)/*.* $(OUTPUTDIR)/
	rm $(CCSMPHOME)/bin/$(OS)/$(ARCH)/*.a
	rm $(CCSMPHOME)/bin/$(OS)/$(ARCH)/*.o

HelloWorldPub : HelloWorldPub.o $(DEPENDS)
	$(CXX) -o $(OUTPUTDIR)/$@ $(OUTPUTDIR)/$^ $(LINKFLAGS)

HelloWorldSub : HelloWorldSub.o $(DEPENDS)
	$(CXX) -o $(OUTPUTDIR)/$@ $(OUTPUTDIR)/$^ $(LINKFLAGS)

HelloWorldQueuePub : HelloWorldQueuePub.o $(DEPENDS)
	$(CXX) -o $(OUTPUTDIR)/$@ $(OUTPUTDIR)/$^ $(LINKFLAGS)

HelloWorldQueueSub : HelloWorldQueueSub.o $(DEPENDS)
	$(CXX) -o $(OUTPUTDIR)/$@ $(OUTPUTDIR)/$^ $(LINKFLAGS)

HelloWorldWebPub : HelloWorldWebPub.o $(DEPENDS)
	$(CXX) -o $(OUTPUTDIR)/$@ $(OUTPUTDIR)/$^ $(LINKFLAGS)

HelloWorldWebSub : HelloWorldWebSub.o $(DEPENDS)
	$(CXX) -o $(OUTPUTDIR)/$@ $(OUTPUTDIR)/$^ $(LINKFLAGS)
