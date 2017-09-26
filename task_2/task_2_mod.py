################################################################################
# Global
################################################################################
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

