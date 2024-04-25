#!/usr/bin/python3

# imports
import psycopg2
import sqlalchemy
import os
import datetime
import zipfile
import io
import json

################################################################################
# helper functions
################################################################################

def remove_nulls(s):
    r'''
    Postgres doesn't support strings with the null character \x00 in them, but twitter does.
    This helper function replaces the null characters with an escaped version so that they can be loaded into postgres.
    Technically, this means the data in postgres won't be an exact match of the data in twitter,
    and there is no way to get the original twitter data back from the data in postgres.

    The null character is extremely rarely used in real world text (approx. 1 in 1 billion tweets),
    and so this isn't too big of a deal.
    A more correct implementation, however, would be to *escape* the null characters rather than remove them.
    This isn't hard to do in python, but it is a bit of a pain to do with the JSON/COPY commands for the denormalized data.
    Since our goal is for the normalized/denormalized versions of the data to match exactly,
    we're not going to escape the strings for the normalized data.

    >>> remove_nulls('\x00')
    '\\x00'
    >>> remove_nulls('hello\x00 world')
    'hello\\x00 world'
    '''
    if s is None:
        return None
    else:
        return s.replace('\x00','\\x00')


#def get_id_urls(url):
#    '''
#    Given a url, returns the corresponding id in the urls table.
#    If no row exists for the url, then one is inserted automatically.
#   '''
#    sql = sqlalchemy.sql.text('''
#    insert into urls 
#        (url)
#        values
#        (:url)
#    on conflict do nothing
#    returning id_urls
#    ;
#    ''')
#    res = connection.execute(sql,{'url':url}).first()
#    if res is None:
#        sql = sqlalchemy.sql.text('''
#        select id_urls 
#        from urls
#        where
#            url=:url
#        ''')
#        res = connection.execute(sql,{'url':url}).first()
#    id_urls = res[0]
#    return id_urls



        try:
            urls = tweet['extended_tweet']['entities']['urls']
        except KeyError:
            urls = tweet['entities']['urls']

        for url in urls:
            tweet_urls.append({
                'id_tweets':tweet['id'],
                'urls':url['expanded_url'],
                })

        ########################################
        # insert into the tweet_mentions table
        ########################################

        try:
            mentions = tweet['extended_tweet']['entities']['user_mentions']
        except KeyError:
            mentions = tweet['entities']['user_mentions']

        for mention in mentions:
            users_unhydrated_from_mentions.append({
                'id_users':mention['id'],
                'name':remove_nulls(mention['name']),
                'screen_name':remove_nulls(mention['screen_name']),
                })

            tweet_mentions.append({
                'id_tweets':tweet['id'],
                'id_users':mention['id']
                })

        ########################################
        # insert into the tweet_tags table
        ########################################

        try:
            hashtags = tweet['extended_tweet']['entities']['hashtags'] 
            cashtags = tweet['extended_tweet']['entities']['symbols'] 
        except KeyError:
            hashtags = tweet['entities']['hashtags']
            cashtags = tweet['entities']['symbols']

        tags = [ '#'+hashtag['text'] for hashtag in hashtags ] + [ '$'+cashtag['text'] for cashtag in cashtags ]

        for tag in tags:
            tweet_tags.append({
                'id_tweets':tweet['id'],
                'tag':remove_nulls(tag)
                })

        ########################################
        # insert into the tweet_media table
        ########################################

        try:
            media = tweet['extended_tweet']['extended_entities']['media']
        except KeyError:
            try:
                media = tweet['extended_entities']['media']
            except KeyError:
                media = []

        for medium in media:
            tweet_media.append({
                'id_tweets':tweet['id'],
                'urls':medium['media_url'],
                'type':medium['type']
                })

    ######################################## 
    # STEP 2: perform the actual SQL inserts
    ######################################## 
    while True:
        try:
            # with connection.begin() as trans:

            # use the bulk_insert function to insert most of the data
            bulk_insert(connection, 'users', users)
            bulk_insert(connection, 'users', users_unhydrated_from_tweets)
            bulk_insert(connection, 'users', users_unhydrated_from_mentions)
            bulk_insert(connection, 'tweet_mentions', tweet_mentions)
            bulk_insert(connection, 'tweet_tags', tweet_tags)
            bulk_insert(connection, 'tweet_media', tweet_media)
            bulk_insert(connection, 'tweet_urls', tweet_urls)

            # the tweets data cannot be inserted using the bulk_insert function because
            # the geo column requires special SQL code to generate the column;
            #
            # NOTE:
            # in general, it is a good idea to avoid designing tables that require special SQL on the insertion;
            # it makes your python code much more complicated,
            # and is also bad for performance;
            # I'm doing it here just to help illustrate the problems
            sql = sqlalchemy.sql.text('''
            INSERT INTO tweets
                (id_tweets,id_users,created_at,in_reply_to_status_id,in_reply_to_user_id,quoted_status_id,geo,retweet_count,quote_count,favorite_count,withheld_copyright,withheld_in_countries,place_name,country_code,state_code,lang,text,source)
                VALUES
                '''
                +
                ','.join([f"(:id_tweets{i},:id_users{i},:created_at{i},:in_reply_to_status_id{i},:in_reply_to_user_id{i},:quoted_status_id{i},ST_GeomFromText(:geo_str{i} || '(' || :geo_coords{i} || ')'), :retweet_count{i},:quote_count{i},:favorite_count{i},:withheld_copyright{i},:withheld_in_countries{i},:place_name{i},:country_code{i},:state_code{i},:lang{i},:text{i},:source{i})" for i in range(len(tweets))])
                )
            res = connection.execute(sql, { key+str(i):value for i,tweet in enumerate(tweets) for key,value in tweet.items() })

        except sqlalchemy.exc.OperationalError as e:
            print(f"e={e}")
            continue
        break

if __name__ == '__main__':

    # process command line args
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--db',required=True)
    parser.add_argument('--inputs',nargs='+',required=True)
    parser.add_argument('--batch_size',type=int,default=1000)
    args = parser.parse_args()

    # create database connection
    engine = sqlalchemy.create_engine(args.db, connect_args={
        'application_name': 'load_tweets.py --inputs '+' '.join(args.inputs),
        })
    connection = engine.connect()

    # loop through file
    # NOTE:
    # we reverse sort the filenames because this results in fewer updates to the users table,
    # which prevents excessive dead tuples and autovacuums
    with connection.begin() as trans:
        for filename in sorted(args.inputs, reverse=True):
            with zipfile.ZipFile(filename, 'r') as archive: 
                print(datetime.datetime.now(),filename)
                for subfilename in sorted(archive.namelist(), reverse=True):
                    with io.TextIOWrapper(archive.open(subfilename)) as f:
                        tweets = []
                        for i,line in enumerate(f):
                            tweet = json.loads(line)
                            tweets.append(tweet)
                        insert_tweets(connection,tweets,args.batch_size)
