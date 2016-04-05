#!/bin/bash
hadoop fs -rm /bigdata/STANKMIC_task1.txt
hadoop fs -mv /user/stankmic/STANKMIC_task1/part-r-00000 /bigdata/STANKMIC_task1.txt
hadoop fs -rm -r /user/stankmic/STANKMIC_task1

