CXXFLAGS=-Wall -std=c++11 -g -O3 
#CXXFLAGS=-Wall -std=c++11 -g -pg
#CXXFLAGS=-Wall -std=c++11 -g -pg -DDEBUG
CC=g++

hello_world:src/betree.hpp test/hello_world.cpp
	$(CC) src/betree.hpp test/hello_world.cpp -o hello_world

hello_world:src/betree.hpp test/test.cpp
	$(CC) src/betree.hpp test/test.cpp -o test

clean:
	$(RM) *.o test