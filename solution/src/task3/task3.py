from task2.task2 import get_denormalized_view
from task1.task1 import logger
import pandas as pd

pd.options.mode.chained_assignment = None


if __name__ == '__main__':
    df = get_denormalized_view()
    df = df[['ts', 'id', 'account_id', 'card_id', 'savings_account_id', 'credit_used', 'balance']]
    df['previous_credit_used'] = df.groupby(['account_id', 'card_id'])['credit_used'].shift(1)
    df['previous_sa_balance'] = df.groupby(['account_id', 'savings_account_id'])['balance'].shift(1)

    df.loc[
           ( ~pd.isna( df['previous_credit_used']) & ~pd.isna(df['credit_used']) & (df['credit_used'] != df['previous_credit_used']) ) |
           ( ~pd.isna(df['previous_sa_balance']) & ~pd.isna(df['balance']) & (df['balance'] != df['previous_sa_balance']) ),
           'transaction_flag'] = True

    transactions = df[df['transaction_flag'] == True]

    transactions.loc[(df['previous_credit_used'] != df['credit_used']), 'credit_transaction'] = df['credit_used'] - df['previous_credit_used']
    transactions.loc[(df['previous_sa_balance'] != df['balance']), 'sa_transaction'] = df['balance'] - df['previous_sa_balance']
    transactions = transactions[['ts', 'id', 'account_id', 'card_id', 'savings_account_id', 'credit_transaction', 'sa_transaction']]
    all_transactions = transactions['credit_transaction'].append(transactions['sa_transaction']).dropna()

    credit_transactions = transactions[~pd.isna(transactions['credit_transaction'])].drop('sa_transaction', axis=1)
    sa_transactions = transactions[~pd.isna(transactions['sa_transaction'])].drop('credit_transaction', axis=1)
    transactions = credit_transactions.append(sa_transactions).sort_values(by='ts', ascending=True)
    logger.info(f'{len(all_transactions)} transactions has been made.')
    logger.info(f'Here is the list of them:\n{transactions}')
