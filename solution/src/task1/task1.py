import pandas as pd
import os
import json
import logging
from copy import deepcopy

# init logger
logger = logging.getLogger()
ch = logging.StreamHandler()
ch.setLevel(os.environ['LOG_LEVEL'])
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.setLevel('DEBUG')

# set pandas to not truncate output tables when print
pd.set_option("max_columns", None)  # show all cols
pd.set_option('max_colwidth', None)  # show full width of showing cols
pd.set_option("expand_frame_repr", False)  # print


def get_df(table_name):
    # read each data file from folder and append to a list of dicts
    # then create a pandas dataframe and return it
    path = os.path.join(os.environ['DATA_PATH'], table_name)
    data_files = [pos_json for pos_json in os.listdir(path) if pos_json.endswith('.json')]
    if len(data_files) == 0:
        raise Exception('No input data found!')
    js_list = []

    for one in data_files:
        one_path = os.path.join(path, one)
        with open(one_path) as f:
            data = f.read()
            chunk = json.loads(data)
            js_list.append(chunk)
    df = pd.DataFrame.from_dict(js_list)

    return df


def create_table_from_events(df):
    # for each event append old state to historical table and make changes to data table
    historical_table = {}  # will contain id + ts key for historical values
    data_table = {}
    entry_id = None
    for k, v in df.iterrows():
        v['ts'] = int(v['ts'])
        if v['op'] == 'c':
            if entry_id is not None:
                historical_table[(entry_id, data_table[entry_id]['ts'])] = deepcopy(data_table[entry_id])
            entry_id = v['id']
            logger.debug(f"create operation for {entry_id} with data {v['data']}")
            v['data']['ts'] = v['ts']
            data_table[entry_id] = v['data']

        elif v['op'] == 'u':
            historical_table[(entry_id, data_table[entry_id]['ts'])] = deepcopy(data_table[entry_id])
            entry_id = v['id']
            v['set']['ts'] = v['ts']
            logger.debug(f"update operation for {entry_id} set {v['set']}")

            for key, value in v['set'].items():
                data_table[entry_id][key] = value

    historical_table[(entry_id, data_table[entry_id]['ts'])] = data_table[entry_id]

    historical_df = pd.DataFrame.from_dict(historical_table, orient='index').reset_index()
    historical_df = historical_df.drop('ts', axis=1).rename({'level_0': 'id', 'level_1': 'ts'}, axis=1)
    return pd.DataFrame.from_dict(data_table, orient='index'), historical_df


def get_accounts():
    df = get_df('accounts')
    df = df.sort_values(by='ts', ascending=True)
    return create_table_from_events(df)


def get_cards():
    cards = get_df('cards')
    cards = cards.sort_values(by='ts', ascending=True)
    return create_table_from_events(cards)


def get_savings_accounts():
    savings_accounts = get_df('savings_accounts')
    savings_accounts = savings_accounts.sort_values(by='ts', ascending=True)
    return create_table_from_events(savings_accounts)


if __name__ == '__main__':
    logger.debug('Task1 initialized.')

    # I am still not sure what __historical table__ mean, let me print both current state and historical
    accounts, historical_accounts = get_accounts()
    logger.info(f"historical accounts table:\n{historical_accounts}\n")
    logger.info(f"accounts table:\n{accounts}\n")

    cards, historical_cards = get_cards()
    logger.info(f"historical cards table:\n{historical_cards}\n")
    logger.info(f"cards table:\n{cards}\n")

    savings_accounts, historical_savings_accounts = get_savings_accounts()
    logger.info(f"historical savings_account table:\n{historical_savings_accounts}\n")
    logger.info(f"savings_accounts table:\n{savings_accounts}\n")

    logger.debug('success')
