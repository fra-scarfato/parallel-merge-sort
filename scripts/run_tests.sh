#!/bin/bash
NODES=(1 2 4 8)
PAYLOAD=(1 24 256)
THREADS=(1 2 4 8 16 32)
SIZES=(1000000 10000000 100000000)
REPEATS=3
LOCAL=0

SEQ_OUT="seq.log"
SIMPLE_OUT="simple_farm.log"
DOUBLE_OUT="double_farm.log"
MPI_OUT="mpi.log"



while getopts "l" opt; do
  case $opt in
    l)
      LOCAL=1
      ;;
    *)
      echo "Usage: $0 [-l]"
      exit 1
      ;;
  esac
done

for ((r=0; r<REPEATS; r++)); do
  for S in "${SIZES[@]}"; do
    for P in "${PAYLOAD[@]}"; do
      if [[ $LOCAL -eq 1 ]]; then
        seq_output=$(./merge_sort_seq $S $P)
      else
        seq_output=$(srun --time=00:10:00 ./merge_sort_seq $S $P)
      fi
      time_seq=$(echo "$seq_output" | awk '{print $3}')
      printf "Size: %d | Payload: %d | Time: %.4f\n" "$S" "$P" "$time_seq" >> "$SEQ_OUT"

      for T in "${THREADS[@]}"; do
        if [[ $LOCAL -eq 1 ]]; then
          ff_simple=$(./merge_sort_simple -t $T -r $P -s $S)
          ff_double=$(./merge_sort_double -t $T -r $P -s $S) 
        else
          ff_simple=$(srun --time=00:10:00 ./merge_sort_simple -t $T -r $P -s $S)
          ff_double=$(srun --time=00:10:00 ./merge_sort_double -t $T -r $P -s $S)
        fi
        time_ff_simple=$(echo "$ff_simple" | awk '{print $3}')
        time_ff_double=$(echo "$ff_double" | awk '{print $3}')
        printf "Threads: %d | Size: %d | Payload: %d | Time: %.4f\n" "$T" "$S" "$P" "$time_ff_simple" >> "$SIMPLE_OUT"
        printf "Threads: %d | Size: %d | Payload: %d | Time: %.4f\n" "$T" "$S" "$P" "$time_ff_double" >> "$DOUBLE_OUT"
      
        for N in "${NODES[@]}"; do
          if [[ $LOCAL -eq 1 ]]; then
            mpi_output=$(./merge_sort_mpi  -t $T -r $P -s $S)
          else
            mpi_output=$(srun --nodes=$N --ntasks-per-node=1 --time=00:10:00 --mpi=pmix ./merge_sort_mpi  -t $T -r $P -s $S)
          fi
          time_mpi=$(echo "$mpi_output" | awk '{print $3}')
          printf "Nodes: %d | Threads: %d | Size: %d | Payload: %d | Time: %.4f\n" "$N" "$T" "$S" "$P" "$time_mpi" >> "$MPI_OUT"
        done
      done
    done	   
  done  
done
