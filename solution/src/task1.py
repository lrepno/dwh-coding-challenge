import pandas as pd
import os
import json
import logging

logger = logging.getLogger()

ch = logging.StreamHandler()
ch.setLevel(os.environ['LOG_LEVEL'])
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.setLevel('DEBUG')


def get_df(table_name):
    path = os.path.join(os.environ['DATA_PATH'], table_name)
    data_files = [pos_json for pos_json in os.listdir(path) if pos_json.endswith('.json')]

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
    data_table = {}
    for k, v in df.iterrows():
        if v['op'] == 'c':
            entry_id = v['id']
            logger.debug(f"create operation for {entry_id} with data {v['data']}")
            data_table[entry_id] = v['data']

        elif v['op'] == 'u':
            entry_id = v['id']
            logger.debug(f"update operation for {entry_id} set {v['set']}")
            for key, value in v['set'].items():
                data_table[entry_id][key] = value

    return pd.DataFrame.from_dict(data_table, orient='index')


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
    logger.info('accounts table:')
    logger.info(get_accounts())
    logger.info('cards table:')
    logger.info(get_cards())
    logger.info('savings_accounts table:')
    logger.info(get_savings_accounts())
    logger.debug('success')
