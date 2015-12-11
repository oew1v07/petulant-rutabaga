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
# Need to clean data
# - sort out missing values like ":", "", "whitespace"
# - put dates into proper date format
# - make geo_lng and geo_lat numbers

def load_json(filename):
    """ Takes a filename and tries to read it in as a json file"""
    with open(filename, 'r') as f:
        out = load(f)

    # Take out the first element of the list since this is the
    # column values (will screw up queries)
    out = out[1:]

    return out

def insert_into_db(list_of_dicts):
    client = MongoClient()
    db = client.tweets
    db.tweets.insert_many(list_of_dicts)

def unique_users(collection_handle):
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

    count = db.out_uni_users.count()

    print("There are {} unique users".format(count))

    return db.out_uni_users, count

def find_duplicates(collection_handle):
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

    count = db.items_to_delete.count()

    print("There are {} duplicates".format(count))

    print("Deleting duplicates now")

    removed = []

    cur = db.items_to_delete.find()

    for i in cur:
        remove = db.tweets.delete_one({"id" : i['_id']})
        removed.append(remove.deleted_count)
        print("Item removed")

    print("{} items removed".format(sum(removed)))

    return db.items_to_delete, count

def top_ten_tweet_perc(collection_handle, base_collection):
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
        perc = (total_tweets/db.tweets.count())*100

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

    cur = collection_handle.find()
    first = True
    pattern = '%Y-%m-%d %H:%M:%S'

    time_deltas = []

    for document in cur:
        if first is True:
            last_time = datetime.strptime(document["timestamp"],pattern)
            first = False
        else:
            this_time = datetime.strptime(document["timestamp"],pattern)
            time_delta = (last_time - first_time).total_seconds()
            time_deltas.append(time_delta)
            last_time = this_time

    mean_time_delta = np.mean(time_deltas)
    return mean_time_delta

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

    print("The mean text length is {}".format(m_length))
    return m_length

def ngrams(collection_handle, n=1):
    top_ten_ngrams = []
    # Create map function
    if n == 1:
        mapfunction = Code("""function() {
                                        var res = this.text.toLowerCase().replace(/\W+/g, " ");
                                        var texts = res.split(" ");
                                        texts.forEach(function(entry){
                                                                        emit(entry, 1);
                                                                     })

                                         }""")
    else:
        mapfunction = Code("""function() {
                                        var res = this.text.toLowerCase().replace(/\W+/g, "");
                                        var texts = res.split(" ");
                                        for (var i = 0; i < values.length - 1; i++) {

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
        coll = db.unigrams
    else:
        coll = db.bigrams

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

    print("The mean number of hashtags per message is {}".format(m_length))
    return m_length
