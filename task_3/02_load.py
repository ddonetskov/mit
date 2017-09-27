#!/usr/bin/env python3.6

import json
import sqlite3

def print_tweet(tweet_json):
    print(tweet_json['user']['screen_name'])
    print(tweet_json['text'])
    if tweet_json['place'] != None:
        print(tweet_json['place']['country_code'])
        print(tweet_json['place']['url'])
    else:
        print(tweet_json['place'])
    print(tweet_json['user']['lang'])
    print(tweet_json['created_at'])
    print(tweet_json['user']['location'])

################################################################################
# Initialization
################################################################################

deleted_count = 0           # number of 'deleted' tweets
created_count = 0           # number of 'created' tweets
failed_count  = 0           # number of tweets failed to process

total_count   = 0           # number of all JSON records

################################################################################
# Main
################################################################################

# going through the lines of key, values

rdb_conn = sqlite3.connect('data/feedback.db')
rdb_cur  = rdb_conn.cursor()

in_file = open('data/three_minutes_tweets.json.txt', 'r')

tn = {}                     # tweet being processed

for tweet_json_str in in_file:
    total_count += 1
    #print('Record #' + str(total_count))
    try:
        tweet_json = json.loads(tweet_json_str)
        tweet_type = list(tweet_json)[0]    
        if tweet_type == 'created_at':
            #print_tweet(tweet_json)
            tn['name']       = tweet_json['user']['screen_name']
            tn['tweet_text'] = tweet_json['text'].lower()
            if tweet_json['place'] != None:
                tn['country_code'] = str(tweet_json['place']['country_code'].upper())
                tn['display_url']  = str(tweet_json['place']['url'])
            else:
                tn['country_code'] = ''
                tn['display_url']  = ''
            tn['lang']       = str(tweet_json['user']['lang']).upper()
            tn['created_at'] = str(tweet_json['created_at'])
            tn['location']   = str(tweet_json['user']['location'].strip().title())
            rdb_cur.execute('insert into tweet(name, tweet_text, country_code, display_url, lang, created_at, location) values (?, ?, ?, ?, ?, ?, ?)', 
                [tn['name'], tn['tweet_text'], tn['country_code'], tn['display_url'], tn['lang'], tn['created_at'], tn['location']])
            created_count += 1
        elif tweet_type == 'delete':
            deleted_count += 1
        else:
            raise Exception('Unknown tweet type')
        if created_count % 1000 == 0:
            rdb_conn.commit()
    except:
        failed_count += 1

in_file.close()

rdb_conn.commit()
rdb_conn.close()

print('Created tweets:                  ' + str(created_count))
print('Deleted tweets:                  ' + str(deleted_count))
print('Failed:                          ' + str(failed_count))
print('Total tweets (sum of the above): ' + str(created_count + deleted_count + failed_count))
print('Total tweets (the direct count): ' + str(total_count))
