CXX                = g++ -std=c++20
MPICXX             = mpicxx -std=c++20
OPTFLAGS	   = -O3 -ffast-math
CXXFLAGS          += -Wall 

INCLUDES	   = -I. -I./include -I./fastflow
LIBS               = -pthread

SEQ_SRC		  := merge_sort_seq.cpp
FF_SRC		  := merge_sort_ff.cpp
MPI_SRC		  := merge_sort_mpi.cpp

SEQ_BIN           := merge_sort_seq
FF_SIMPLE         := merge_sort_simple
FF_DOUBLE         := merge_sort_double
MPI_BIN		  := merge_sort_mpi

.PHONY: all clean cleanall 
all: $(SEQ_BIN) $(FF_SIMPLE) $(FF_DOUBLE) $(MPI_BIN)

# Sequential version
$(SEQ_BIN): $(SEQ_SRC)
	$(CXX) $(INCLUDES) $(CXXFLAGS) $(OPTFLAGS) -o $@ $< $(LIBS)

# FastFlow simple farm (no -DDOUBLE_FARM)
$(FF_SIMPLE): $(FF_SRC)
	$(CXX) $(INCLUDES) $(CXXFLAGS) $(OPTFLAGS) -o $@ $< $(LIBS)

# FastFlow double farm (with -DDOUBLE_FARM)
$(FF_DOUBLE): $(FF_SRC)
	$(CXX) $(INCLUDES) $(CXXFLAGS) $(OPTFLAGS) -DDOUBLE_FARM -o $@ $< $(LIBS)

# MPI version
$(MPI_BIN): $(MPI_SRC)
	$(MPICXX) $(INCLUDES) $(CXXFLAGS) $(OPTFLAGS) -o $@ $< $(LIBS) -lm

clean: 
	-rm -fr *.o *~
cleanall: clean
	-rm -fr $(TARGET)
