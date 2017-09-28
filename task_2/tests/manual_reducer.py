#!/usr/bin/env python3.6

import datetime
import pyarrow as pa
import re
import sys

import t2_common as g

################################################################################
# Initialization
################################################################################

if g.DEST_FS_TYPE == 'FS':
    pass
elif g.DEST_FS_TYPE == 'HDFS':
    hdfs = pa.hdfs.connect()
else:
    raise Exception("Unknown file system.")

################################################################################
# Main
################################################################################

# going through the lines of key, values

prev_key = None
out_file = None

for line in sys.stdin:

    key, value = line.split('\t')

    # print('Processing ' + key)

    if key != prev_key:
        if out_file != None:
            out_file.close();
        # create the new file according to the date
        key_date = datetime.datetime.strptime(key, '%Y-%m-%d')
        out_file_name = g.ARCHIVE_DIR + '/' + str(key_date.year) + '/' + str(key_date.month) + '/' + str(key_date.day) + '/data.csv'
        out_file = hdfs.open(out_file_name, 'wb')
        print('Created the file ' + out_file_name)
     
    out_file.write(value)

    prev_key = key

if out_file != None:
    out_file.close();
