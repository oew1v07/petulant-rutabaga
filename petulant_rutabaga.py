"""This script will take a csv file and put it into a mongo database. It will
also run specified queries"""

from pymongo import MongoClient
import pandas as pd

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

def insert_into_db(list_of_dicts):
    client = MongoClient()
    db = client.test
