#!/usr/bin/env python3.6

import sqlite3

################################################################################
# Initialization
################################################################################

rdb_conn = sqlite3.connect('data/feedback.db')
rdb_cur  = rdb_conn.cursor()
rdb_cur2 = rdb_conn.cursor()

################################################################################
# Main
################################################################################

# going through the lines of key, values

# load the AFINN dictionary
AFINN = {}
with open('data/AFINN-111.txt', 'r') as AFINN_file:
    for line in AFINN_file:
        AFINN[line.split('\t')[0].strip()] = int(line.split('\t')[1])

# going through the EN messages, calculating the overall tweet sentiment
processed_count = 0
for row in rdb_cur.execute('select rowid, tweet_text from tweet where upper(lang) = "EN"'):
    tweet_sentiment = 0
    for word in row[1].split():
        tweet_sentiment += int(AFINN.get(word, 0))
    #print('Sentiment: ' + str(sentiment))
    rdb_cur2.execute('update tweet set tweet_sentiment = ? where rowid = ?', [tweet_sentiment, row[0]])
    processed_count += 1

rdb_conn.commit()
rdb_conn.close()

print('Processed: ' + str(processed_count))
