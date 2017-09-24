#!/usr/bin/env python3.6

import csv
import datetime
import random

import pyarrow as pa

################################################################################
# Global
################################################################################
DEST_FS_TYPE = 'HDFS'

################################################################################
# Initialization
################################################################################

# initialize the randomizer, it's required to generate the data
random.seed()

if DEST_FS_TYPE == 'FS':
    pass
elif DEST_FS_TYPE == 'HDFS':
    hdfs = pa.hdfs.connect()
else:
    raise Exception("Unknown file system.")

#fieldnames = ['id', 'type', 'value', 'business_date']
#csvwriter = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=',', quoting=csv.QUOTE_NONNUMERIC)
#csvwriter.writeheader()

################################################################################
# Generation
################################################################################

for fn in range(1,10):
    if DEST_FS_TYPE == 'FS':
        file_name = 'data/data' + str(fn) + '.csv'
        csvfile = open(file_name, 'w', newline='')
    elif DEST_FS_TYPE == 'HDFS':
        file_name = '/mts/task_2/incoming/' + str(fn) + '.csv'
        csvfile = hdfs.open(file_name, 'wb')
    print('Created the ' + DEST_FS_TYPE + ' file ' + file_name + '. Writing data into it...', end='')
    csvwriter = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_NONNUMERIC)
    csvfile.write('id, type, value, business_date\n')    
    for i in range(0,100):
        # generate a random row confirming to the format
        csv_row = [random.randint(1,9),
                   random.choice(['pay', 'rec']),
                   random.randint(-10000, 10000),
                   (datetime.date(2016, 1, 1) + datetime.timedelta(random.randint(0,500))).isoformat()]
        # except the first file, randomly breaking the format for one row per ten by slicing out a part of the sequence
        if fn != 1 and random.randrange(10) == 6:
            del(csv_row[random.randrange(4)])
        # saving the str into CSV
        csvwriter.writerow(csv_row)
    print('Done.')
    csvfile.close()

################################################################################
# Finalization
################################################################################

