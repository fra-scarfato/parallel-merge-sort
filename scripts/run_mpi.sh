#!/bin/bash
NODES=(2 4 8)
PAYLOAD=(1)
THREADS=(1)
SIZES=(20000000 40000000 80000000)
REPEATS=1

SEQ_OUT="seq.log"
SIMPLE_OUT="simple_farm.log"
DOUBLE_OUT="double_farm.log"
MPI_OUT="mpi.log"

for ((r=0; r<REPEATS; r++)); do
  for S in "${SIZES[@]}"; do
    for P in "${PAYLOAD[@]}"; do
      for T in "${THREADS[@]}"; do
        for N in "${NODES[@]}"; do
          mpi_output=$(srun --nodes=$N --ntasks-per-node=1 --time=00:10:00 --mpi=pmix ./merge_sort_mpi  -t $T -r $P -s $S)
          time_mpi=$(echo "$mpi_output" | awk '{print $3}')
          printf "Nodes: %d | Threads: %d | Size: %d | Payload: %d | Time: %.4f\n" "$N" "$T" "$S" "$P" "$time_mpi" >> "$MPI_OUT"
        done
      done
    done	   
  done  
done
