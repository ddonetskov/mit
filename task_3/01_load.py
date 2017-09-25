#!/usr/bin/env python3.6

import sqlite3
import json

################################################################################
# Initialization
################################################################################

rdb_conn = sqlite3.connect('data/feedback.db')
rdb_cur  = rdb_conn.cursor()

################################################################################
# Main
################################################################################

# going through the lines of key, values

in_file = open('data/three_minutes_tweets.json.txt', 'r')

i = 0
deleted_count = 0
created_count = 0
for tweet_json_str in in_file:
    i += 1
    tweet_json = json.loads(tweet_json_str)
    tweet_type = list(tweet_json)[0]    
    if tweet_type == 'created_at':
        created_count += 1
        #print('Record #' + str(created_count))
        #print(tweet_json['user']['screen_name'])
        #print(tweet_json['text'])
        #print(tweet_json['user']['time_zone'])
        #print(tweet_json['source'])
        #print(tweet_json['user']['lang'])
        #print(tweet_json['created_at'])
        #print(tweet_json['user']['location'])
        rdb_cur.execute('insert into tweet(name, tweet_text, country_code, display_url, lang, created_at, location) values (?, ?, ?, ?, ?, ?, ?)', [
            str(tweet_json['user']['screen_name']),
            str(tweet_json['text']),
            str(tweet_json['user']['time_zone']).upper(),
            str(tweet_json['source']),
            str(tweet_json['user']['lang']).upper(),
            str(tweet_json['created_at']),
            str(tweet_json['user']['location'])])
    elif tweet_type == 'delete':
        deleted_count += 1
    else:
        raise Exception('Unknown tweet type')
    if created_count % 1000 == 0:
        rdb_conn.commit()

in_file.close()

rdb_conn.commit()
rdb_conn.close()

print('Deleted tweets: ' + str(deleted_count))
print('Created tweets: ' + str(created_count))
