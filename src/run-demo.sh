#!/bin/bash

##Remove old netsort executable
rm -f netsort

##Build netsort
go build -o netsort netsort.go

##Run for process
for i in $(seq 0 3)
do
  SERVER_ID=$i
  INPUT_FILE_PATH='testcases/testcase1/input-'${SERVER_ID}'.dat'
  OUTPUT_FILE_PATH='testcases/testcase1/output-'${SERVER_ID}'.dat'
  CONFIG_FILE_PATH='testcases/testcase1/config.yaml'
  echo "./netsort ${SERVER_ID} ${INPUT_FILE_PATH} ${OUTPUT_FILE_PATH} ${CONFIG_FILE_PATH}"
  nohup ./netsort ${SERVER_ID} ${INPUT_FILE_PATH} ${OUTPUT_FILE_PATH} ${CONFIG_FILE_PATH} > "logs/$(date +"%Y%m%d%H%M%S")_server_${SERVER_ID}.log" &
done

wait
