from task1.task1 import get_cards, get_accounts, get_savings_accounts
import pandas as pd
from datetime import datetime

# set pandas to not truncate output tables when print
pd.set_option("max_columns", None)  # show all cols
pd.set_option('max_colwidth', None)  # show full width of showing cols
pd.set_option("expand_frame_repr", False)  # print


def get_denormalized_view():
    _, historical_accounts = get_accounts()
    _, historical_cards = get_cards()
    _, historical_savings_accounts = get_savings_accounts()

    # ok it might be not the best one, but it is relatively easy to do
    # collect all unique timestamps
    # join using ts + id with each of the table, keeping the outers
    ts_series = historical_accounts['ts']
    ts_series = ts_series.append(historical_cards['ts'])
    ts_series = ts_series.append(historical_savings_accounts['ts'])
    ts_series = ts_series.drop_duplicates()

    ts_df = pd.DataFrame(ts_series).sort_values(by='ts', ascending=True)
    df = ts_df.merge(historical_accounts,
                     'outer',
                     left_on=['ts'],
                     right_on=['ts'],
                     suffixes=('', '_accounts'))
    df.fillna(method='ffill', inplace=True)

    df = df.merge(historical_cards,
                  'outer',
                  left_on=[df['ts'], df['card_id']],
                  right_on=[historical_cards['ts'], historical_cards['card_id']],
                  suffixes=('', '_card')) \
        .drop('key_0', axis=1) \
        .drop('key_1', axis=1)

    historical_savings_accounts['ts'] = historical_savings_accounts['ts'].astype('float64')
    df.fillna(method='ffill', inplace=True)
    df = df.merge(historical_savings_accounts,
                  'outer',
                  left_on=[df['ts'], df['savings_account_id']],
                  right_on=[historical_savings_accounts['ts'], historical_savings_accounts['savings_account_id']],
                  suffixes=('', '_savings')) \
        .drop('key_0', axis=1) \
        .drop('key_1', axis=1)

    df.fillna(method='ffill', inplace=True)
    return df


if __name__ == '__main__':
    print(get_denormalized_view())
