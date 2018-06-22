SRC_COMM =  appsyncredisclient.cpp crc16.cpp slotdistribution.cpp \
	    syncredisclusterclient.cpp 
	

OBJ_COMM = $(SRC_COMM:.cpp=.o)

CXXFLAG =-g -O0 -Wall  -Wno-deprecated
LINK_CXXFLAG = -static

REDIS_CLUSTER_CLIENT_LIB=libredisclusterclient.a

.SUFFIXES: .o .cpp
.cpp.o:
	$(CXX) $(CXXFLAG) -c -o $@ $<

all: $(REDIS_CLUSTER_CLIENT_LIB) 

$(REDIS_CLUSTER_CLIENT_LIB):$(OBJ_COMM)
	ar -ru $(REDIS_CLUSTER_CLIENT_LIB) $(OBJ_COMM)
	
clean: 
	rm -f *.o *.ro $(PROGRAM) $(REDIS_CLUSTER_CLIENT_LIB)
	
rebuild:clean all
