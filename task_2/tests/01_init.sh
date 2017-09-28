#!/bin/sh

export PYTHONPATH=$HOME/Projects/task_2

hadoop fs -mkdir -p '/mts/task_2'
hadoop fs -mkdir -p '/mts/task_2/incoming'
hadoop fs -mkdir -p '/mts/task_2/staging'
hadoop fs -mkdir -p '/mts/task_2/rejected'
hadoop fs -mkdir -p '/mts/task_2/archive'

