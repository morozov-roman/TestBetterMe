# preparing data in csv and upload to database

import sqlite3
import csv
import hashlib
import pandas as pd
import requests


def txt_to_csv(file_path):
    """ convert .txt files to .csv format and prepare data
    :param file_path: list of paths type:string
    :return:
    """
    for file in file_path:
        df_txt = pd.read_csv(file, sep="\t")

        df_date = df_txt['Event Date'].unique()
        df_date.sort()
        # Requesting exchange rates
        try:
            url = f'https://api.exchangerate.host/timeseries?start_date={df_date[0]}&end_date={df_date[-1]}'
            response = requests.get(url)
        except Exception as e:
            print(e)
            return

        data = response.json()

        df_rates_by_date = pd.DataFrame(data["rates"])

        df_proceeds_usd = pd.Series([], dtype='float64')
        # Calculating income in USD
        for i, row in df_txt.iterrows():
            refund = 1
            if row[21] == 'Yes':
                refund = -1
            if row[13] == 'EUR':
                proceeds_usd = (row[12] * df_rates_by_date.loc['USD', row[0]]) * refund
            elif row[13] == 'USD':
                proceeds_usd = row[12] * refund
            else:
                proceeds_usd = ((row[12] / df_rates_by_date.loc[row[13], row[0]]) * df_rates_by_date.loc[
                    'USD', row[0]]) * refund
            df_proceeds_usd = pd.concat([df_proceeds_usd, pd.Series(float('{:.2f}'.format(proceeds_usd)))],
                                        ignore_index=True)

        df_txt.insert(14, 'Proceeds USD', df_proceeds_usd)

        df_txt.to_csv(file[:-4] + '.csv', index=None)


def connect_database(db_file):
    """ create a database connection to the SQLite database
                specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    try:
        connection = sqlite3.connect(db_file)
        return connection
    except Exception as e:
        print(e)


def create_table(connection, create_table_sql):
    """ create a table from the create_table_sql statement
    :param connection: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = connection.cursor()
        c.execute(create_table_sql)
        connection.commit()
    except Exception as e:
        print(e)


def add_to_database(connection, list_of_paths):
    """ add data to the table
    :param connection: Connection object
    :param list_of_paths: list of paths to data files
    :return:
    """
    for path in list_of_paths:
        try:
            with open(path, 'r') as f:
                reader = csv.reader(f)
                columns = next(reader)
                query = 'INSERT OR FAIL INTO {0} ({1}) VALUES ({2})'
                query = query.format('report', ','.join(['hash_row',
                                                         'event_date',
                                                         'app_name',
                                                         'app_apple_ID',
                                                         'subscription_name',
                                                         'subscription_apple_ID',
                                                         'subscription_group_ID',
                                                         'subscription_duration',
                                                         'introductory_price_type',
                                                         'introductory_price_duration',
                                                         'marketing_opt_in_duration',
                                                         'customer_price',
                                                         'customer_currency',
                                                         'developer_proceeds',
                                                         'proceeds_currency',
                                                         'proceeds_usd',
                                                         'preserved_pricing',
                                                         'proceeds_reason',
                                                         'client',
                                                         'device',
                                                         'country',
                                                         'subscriber_ID',
                                                         'subscriber_ID_reset',
                                                         'refund',
                                                         'purchase_date',
                                                         'units'
                                                         ]), ','.join('?' * (len(columns) + 1)))
                cursor = connection.cursor()
                for data in reader:
                    data.insert(0, hashlib.sha256(str(data).encode()).hexdigest())
                    try:
                        cursor.execute(query, data)
                        connection.commit()
                    except Exception as e:
                        print(data)
                        print(e)

        except Exception as e:
            print(e)
