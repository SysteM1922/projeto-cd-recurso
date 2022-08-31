#!/bin/bash

for ((i=0; i<$1; i++)); do
	sleep 0.01
    python3 worker.py -b 5000 &
done