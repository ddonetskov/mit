#!/usr/bin/env python3.6

import pyarrow as pa

DEST_FS_TYPE = 'HDFS'

if DEST_FS_TYPE == 'FS':
    raise Exception("Unsupported.")
elif DEST_FS_TYPE == 'HDFS':
    INCOMING_DIR = '/mts/task_2/incoming'
    STAGING_DIR  = '/mts/task_2/staging'
    REJECTED_DIR = '/mts/task_2/rejected'
    ARCHIVE_DIR  = '/mts/task_2/archive'
else:
    raise Exception("Unknown file system.")

if __name__ == '__main__':
    print("Testing connection to HDFS")
    hdfs = pa.hdfs.connect()
    print(hdfs.ls('/'))
    hdfs.close()
