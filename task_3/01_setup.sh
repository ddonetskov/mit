#!/bin/sh

mkdir data

sqlite3 data/feedback.db < 01_create_db.sql
