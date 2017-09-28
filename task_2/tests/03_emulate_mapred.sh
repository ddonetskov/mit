#!/bin/sh

tests/sender.py | ./03_mapper.py | sort | tests/manual_reducer.py


