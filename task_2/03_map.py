#!/usr/bin/env python3.6

#import io
#import pyarrow as pa
#import re
import task_2_mod as g
import sys

################################################################################
# Initialization
################################################################################

#if g.DEST_FS_TYPE == 'FS':
#    pass
#elif g.DEST_FS_TYPE == 'HDFS':
#    hdfs = pa.hdfs.connect()
#else:
#    raise Exception("Unknown file system.")

################################################################################
# Main
################################################################################

# going through the list of files
#for in_file_name in hdfs.ls(g.STAGING_DIR):
#
#    # print('Mapping the file: ' + in_file_name + '. ', end='');
#
#    # avoiding the processed files and the rejected files
#    if re.match('.*\.processed$|.*\.rejected$', in_file_name):
#        continue;
#
#    in_file = hdfs.open(in_file_name, 'rb')
#    in_memf = io.StringIO(in_file.read().decode())
#    in_file.close()
#
#    for memf_row in in_memf:
#        # print(memf_row)
#        print(memf_row.split(',')[3][:-2] + '\t' + memf_row[:-2])
#
#    in_memf.close()
#
#    # marking the file as processed
#    hdfs.rename(in_file_name, in_file_name + '.processed')

for line in sys.stdin:
    #print(line.split(',')[3][:-2] + '\t' + line[:-2])
    print(line.split(',')[3] + '\t' + line)


