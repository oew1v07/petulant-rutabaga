"""This script will take a csv file and put it into a mongo database. It will
also run specified queries"""

from pymongo import MongoClient
import pandas as pd
from json import load
from datetime import datetime
import numpy as np
from bson import Code

# First things first load in csv as pandas dataframe for ease of conversion

def load_csv(filename):
    """Takes a csv with a given filename and puts it into a pandas DataFrame

    Parameters
    ----------
    filename: String
        The filepath of the csv file to be loaded

    Returns
    -------
    df: pandas.DataFrame
        The csv written into a dataframe

    """

    # This has to read line by line so that I can work out which line it's going
    # wrong on.

    list_of_dicts = []
    bad_rows = 0

    with open(filename, 'rb') as f:
        # Wrap in try except for ease of troubleshooting.
        # All cases where the length of a row isn't 6 will be ignored as will
        # any that cause byte errors
        for index, line in enumerate(f.readlines()):
            try:
                line = line.decode('utf-8')
                s = line.split(',')
                if len(s) > 6:
                    continue
                else:
                    result = {}
                    try:
                        result['id'] = s[0]
                        result['id_member'] = s[1]
                        result['timestamp'] = s[2]
                        result['text'] = s[3]
                        result['geo_lat'] = s[4]
                        result['geo_lng'] = s[5]

                    except Exception:
                        # If it got this far then there were more than 6 elements
                        # in the data.
                        print("Did not load this item into the list. "
                        " It was row {}".format(index))
                        bad_rows += 1
                    list_of_dicts.append(result)
            except Exception:
                print('It went wrong after line {}'.format(line))
                bad_rows += 1
                continue
            print(line)
    return list_of_dicts, bad_rows
# 1106 lines aren't being put in database.
# list_of_dicts is in data.json


def load_json(filename):
    """ Takes a filename and tries to read it in as a json file"""
    with open(filename, 'r') as f:
        out = load(f)

    # Take out the first element of the list since this is the
    # column values (will screw up queries)
    out = out[1:]

    return out


def access_database():
    client = MongoClient()
    db = client.tweets
    return db


def insert_into_db(database, list_of_dicts):
    database.tweets.insert_many(list_of_dicts)
    return database.tweets


def find_duplicates(collection_handle, database):
    """Finds duplicates within a collection.

    Parameters
    ----------
    collection_handle: pymongo.collection.collection
        The name of the collection to find unique users of. It must have a
        field called id
    """

    collection_handle.aggregate(
        [
            { "$group": { "_id": "$id", "count":{"$sum":1}}},
            { "$match": { "count": 2 } },
            { "$out": "items_to_delete"}
        ], allowDiskUse=True
    );

    count = database.items_to_delete.count()

    print("There are {} duplicates".format(count))

    print("Deleting duplicates now")

    removed = []

    cur = database.items_to_delete.find()

    for i in cur:
        remove = database.tweets.delete_one({"id" : i['_id']})
        removed.append(remove.deleted_count)
        print("Item removed")

    print("{} items removed".format(sum(removed)))

    return database.items_to_delete, count


def unique_users(collection_handle, database):
    """Creates a new collection of unique users with count of tweets, to then query.

    Parameters
    ----------
    collection_handle: pymongo.collection.collection
        The name of the collection to find unique users of. It must have a
        field called id
    """

    cur = collection_handle.aggregate(
        [
            { "$group": { "_id": "$id_member", "count":{"$sum":1}}},
            { "$sort": { "count": -1 } },
            { "$out": "out_uni_users"}
        ], allowDiskUse=True
    )

    count = database.out_uni_users.count()

    print("There are {} unique users".format(count))

    return database.out_uni_users, count


def top_ten_tweet_perc(collection_handle, database):
    """Calculates percentage of tweets published by top ten users

    Parameters
    ----------
    collection_handle: pymongo.collection.Collection
        This must be the handle to the collection created in unique_users.
        Or a collection that has "id" and "count of tweets". It also must be
        sorted descending by "count of tweets"
    base_collection: pymongo.collection.Collection
        The original fully populated database.
    """

    cur = collection_handle.aggregate(
        [
            { "$limit": 10},
            { "$group":
                {
                    "_id": 1,
                    "tweets_10":{"$sum":"$count"}
                }
            }
        ]
    )

    for document in cur:
        total_tweets = document["tweets_10"]
        perc = (total_tweets/database.tweets.count())*100

    print("The top 10 tweeters posted %.1f%% of the tweets" % perc)

    return total_tweets, perc


def first_and_last(collection_handle):
    cur = collection_handle.aggregate(
        [
            { "$sort": { "timestamp":1 }},
            { "$group":
                {
                    "_id": 1,
                    "first_time":{"$first":"$timestamp"},
                    "last_time":{"$last":"$timestamp"}
                }
            }
        ], allowDiskUse=True
    )
    for document in cur:
        first_time = document["first_time"]
        last_time = document["last_time"]
        print("The first tweet was posted "
              "at {}, and the last at {}".format(first_time, last_time))

    return first_time, last_time


def mean_time_delta(collection_handle):
    # Two different stackoverflow questions suggest that this is impossible in
    # pure mongo queries therefore I am using python to be able to do this.

    cur = collection_handle.aggregate(
        [
            { "$sort": { "timestamp":1 }},
        ], allowDiskUse=True
    )
    first = True
    pattern = '%Y-%m-%d %H:%M:%S'

    time_deltas = []

    for document in cur:
        if first is True:
            last_time = datetime.strptime(document["timestamp"],pattern)
            first = False
        else:
            this_time = datetime.strptime(document["timestamp"],pattern)
            time_delta = (this_time - last_time).total_seconds()
            time_deltas.append(time_delta)
            last_time = this_time

    avg_time_delta = np.mean(time_deltas)
    print("The mean time delta was %.2f seconds" % avg_time_delta)
    return avg_time_delta, time_deltas


def mean_length(collection_handle):

    # Create map function
    mapfunction = Code("""function() {
                                        emit('const', this.text.length);
                                     }""")

    # Create reduce function
    reducefunction = Code("""function(key, values) {
                                                    return Array.sum(values) / values.length;
                                                   }""")

    result = collection_handle.map_reduce(mapfunction, reducefunction,
                                          "myresults", full_reponse=True)

    m_length = (result.find().next())['value']

    print("The mean text length is %.0f" % m_length)
    return m_length


def ngrams(collection_handle, database, n=1):
    top_ten_ngrams = []
    # Create map function
    if n == 1:
        mapfunction = Code("""function() {
                                        var res = this.text.toLowerCase().replace(/[^a-zA-Z_0-9 ]/g, "");
                                        var texts = res.split(" ");
                                        texts.forEach(function(entry){
                                                                        if (entry != "" && entry != " ") {
                                                                        emit(entry, 1);
                                                                        }
                                                                     })

                                         }""")
    else:
        mapfunction = Code("""function() {
                    var res = this.text.toLowerCase().replace(/[^a-zA-Z_0-9 ]/g, "");
                    var texts = res.split(" ");
                    for (var i = 0; i < texts.length - 1; i++) {
                        var res1 = texts[i];
                        var res2 = texts[i+1];
                        if (res1 != "" && res1 != " " && res2 != "" && res2 != " ") {
                        emit(res1.concat(" ", res2), 1);
                        }
                    }

                     }""")


    # Create reduce function
    reducefunction = Code("""function(key, values) {
                                                    return Array.sum(values);
                                                   }""")
    if n == 1:
        name = "unigrams"
    else:
        name = "bigrams"

    result = collection_handle.map_reduce(mapfunction, reducefunction,
                                          name, full_reponse=True)

    if n == 1:
        coll = database.unigrams
    else:
        coll = database.bigrams

    cur = coll.aggregate(
        [
            { "$sort": {"value":-1}},
            { "$limit": 10}
        ]
    )

    for i in cur:
        print(i)
        top_ten_ngrams.append(i['_id'])

    if n == 1:
        word = 'unigram'
    else:
        word = 'bigram'

        print("The most common {}s are :".format(word), top_ten_ngrams)
    return top_ten_ngrams


def mean_hash(collection_handle):

    # Create map function
    mapfunction = Code("""function() {
                      var some = this.text.toLowerCase();
                      var res = some.split(" ");
                      var count = 0;
                      res.forEach(function(entry) {
                                                  if (entry.startsWith("#"))
                                                  {
                                                      count = count + 1;
                                                  }
                                                  }
                                 );
                     emit('test', count);
                     }""")

    # Create reduce function
    reducefunction = Code("""function(key, values) {
                                                    return Array.sum(values) / values.length;
                                                   }""")

    result = collection_handle.map_reduce(mapfunction, reducefunction,
                                          "hashresults", full_reponse=True)

    m_length = (result.find().next())['value']

    print("The mean number of hashtags per message is %.2f" % m_length)
    return m_length

def area_agg(collection_handle, database, dp = 2):
    to_multiply = 10**dp

    mapfunction = Code("""function() {
                       var res_lat = this.geo_lat.replace(/[^0-9.]/g, "");
                       var res_lng = this.geo_lng.replace(/[^0-9.]/g, "");
                       var res1_lat = Math.round(parseFloat(res_lat)*%d)/%d;
                       var res1_lng = Math.round(parseFloat(res_lng)*%d)/%d;
                       var lat = res1_lat.toString();
                       var lng = res1_lng.toString();
                       var latlng = lat.concat(" ", lng);
                       emit(latlng, 1);

                     }""" % (to_multiply, to_multiply, to_multiply, to_multiply))

    reducefunction = Code("""function(key, values) {
                                                    return Array.sum(values);
                                                   }""")

    result = collection_handle.map_reduce(mapfunction, reducefunction,
                                          "geog", full_reponse=True)

   
    coll = database.geog

    cur = coll.aggregate(
        [
            { "$sort": {"value":-1}},
            { "$limit": 10}
        ]
    )

    for i in cur:
        print(i)
        top_ten_places.append(i['_id'])

    print("The most common place are :", top_ten_places[0])

    return top_ten_places


def run_entire_pipeline(filename):
    # Filename is the csvfile to be given
    list_of_dicts, bad_rows = load_csv(filename)

    db = access_database()
    db.tweets = insert_into_db(db, list_of_dicts)
    db.items_to_delete, deleted_count = find_duplicates(db.tweets, db)

    # Query number 1
    database.out_uni_users, unique_count = unique_users(db.tweets, db)

    # Query number 2
    total_tweets_10, top_ten_perc = top_ten_tweet_perc(db.out_uni_users, db)

    # Query number 3
    first_time, last_time = first_and_last(db.tweets)

    # Query number 4
    mean_time_delta = mean_time_delta(db.tweets)

    # Query number 5
    mean_length = mean_length(db.tweets)

    # Query number 6

    top_ten_unigrams = ngrams(db.tweets, db, n=1)

    top_ten_bigrams = ngrams(db.tweets, db, n=2)

    # Query number 7

    mean_hash = mean_hash(db.tweets)


def just_queries():

    db = access_database()

    # Query number 1
    database.out_uni_users, unique_count = unique_users(db.tweets, db)

    # Query number 2
    total_tweets_10, top_ten_perc = top_ten_tweet_perc(db.out_uni_users, db)

    # Query number 3
    first_time, last_time = first_and_last(db.tweets)

    # Query number 4
    mean_time_delta = mean_time_delta(db.tweets)

    # Query number 5
    mean_length = mean_length(db.tweets)

    # Query number 6

    top_ten_unigrams = ngrams(db.tweets, db, n=1)

    top_ten_bigrams = ngrams(db.tweets, db, n=2)

    # Query number 7

    mean_hash = mean_hash(db.tweets)
