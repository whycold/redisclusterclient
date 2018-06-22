CC = gcc
CXX = g++
CC32 = gcc

CXXFLAGS = -Wall -D__STDC_LIMIT_MACROS -O0 -ggdb

LINK_CXXFLAG = $(CXXFLAGS)

INCLUDE = -I..

SRC_COMM = main.cpp 


OBJ_COMM = $(SRC_COMM:.cpp=.o)

LIB = $(COMM_LIB)

.SUFFIXES: .o .cpp
.cpp.o:
	$(CXX) $(CXXFLAGS) $(INCLUDE) -c -o $@ $<

PROGRAM = redis_cluster_client_d #serviced
all: $(PROGRAM)  #serviced

../libredisclusterclient.a : ../Makefile
	make  -C .. -f Makefile

../hiredis/libhiredis.a : ../hiredis/Makefile
	make -C ../hiredis -f Makefile

$(PROGRAM) :  $(OBJ_COMM) ../libredisclusterclient.a  ../hiredis/libhiredis.a
	$(CXX) -o $@ $(LINK_CXXFLAG) $(INCLUDE) $(OBJ_COMM) \
	../libredisclusterclient.a \
	../hiredis/libhiredis.a

clean:
	$(RM) -r $(PROGRAM) 
	rm -f *.o

rebuild:clean all

