#!/bin/sh

hadoop fs -rm '/mts/task_2/incoming/*.processed'
hadoop fs -rm '/mts/task_2/staging/*.processed'
hadoop fs -rm '/mts/task_2/rejected/*'

./01_generate_data.py
./02_prefilter.py

