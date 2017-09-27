#!/usr/bin/env python3.6

import datetime
import io
import re
import pyarrow as pa

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
for in_file_name in hdfs.ls(g.INCOMING_DIR):

    print('Checking the file: ' + in_file_name + '. ', end='');

    if re.match('.*\.processed$', in_file_name):
        print('Ignoring.');
        continue;
    else:
        print('Processing...');

    in_file = hdfs.open(in_file_name, 'rb')
    in_memf = io.StringIO(in_file.read().decode())
    in_file.close()

    # Checking the first line, if that's not the header then ignorying the file at all
    if not re.match('^id, *type, *value, *business_date$', in_memf.readline()):
        print('Skipping the fine as there is no header.')
        continue

    out_file_name = g.STAGING_DIR +  '/' + in_file_name.split('/')[-1]
    rej_file_name = g.REJECTED_DIR + '/' + in_file_name.split('/')[-1]
    out_file = hdfs.open(out_file_name, 'wb')
    rej_file = hdfs.open(rej_file_name, 'wb')

    no_records_matched  = 0
    no_records_rejected = 0

    for memf_row in in_memf:
        # print(memf_row, end='')
        # Checking if that confirms to the format
        if re.match('^[0-9]*, *("pay"|"rec"), *-{0,1}[0-9]+, *"\d{4}-\d{2}-\d{2}"\r\n$', memf_row):
            out_file.write(memf_row)
            no_records_matched += 1
        else:
            rej_file.write(memf_row)
            no_records_rejected += 1

    in_memf.close()
    out_file.close()
    rej_file.close()

    # renaming the input file to '*.processed'
    hdfs.rename(in_file_name, in_file_name + '.processed')

    # removing the 'rejected' file if it's empty
    if no_records_rejected == 0:
        hdfs.rm(rej_file_name)

    print('Records: ' + str(no_records_matched) + ' matched, ' + str(no_records_rejected) + ' rejected')

