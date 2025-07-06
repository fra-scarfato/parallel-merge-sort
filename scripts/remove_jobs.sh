#!/bin/bash

while true; do
  job_id=$(squeue | grep "f.scarfa" | awk '{print $1}')
  scancel $job_id
  sleep 1
done
