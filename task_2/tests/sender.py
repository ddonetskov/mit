#!/usr/bin/env python3.6

import sys

import io
import pyarrow as pa
import re

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

# going through the list of files
for in_file_name in hdfs.ls(g.STAGING_DIR):

    # avoiding the processed files and the rejected files
    if re.match('.*\.processed$|.*\.rejected$', in_file_name):
        # print('Ignorying the file: ' + in_file_name + '. ');
        continue;

    # print('Sending the file: ' + in_file_name + '. ');

    in_file = hdfs.open(in_file_name, 'rb')
    in_memf = io.StringIO(in_file.read().decode())
    in_file.close()

    for memf_row in in_memf:
        print(memf_row[:-2])

    in_memf.close()

    # marking the file as processed
    # hdfs.rename(in_file_name, in_file_name + '.processed')
