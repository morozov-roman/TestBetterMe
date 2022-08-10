# Requirements

  * Python 3.8

> This code makes use of the `f"..."` or [f-string
> syntax](https://www.python.org/dev/peps/pep-0498/). This syntax was
> introduced in Python 3.6.

# Use the packages
```bash
pip install sqlite3
pip install csv
pip install hashlib
pip install pandas as pd
pip install requests
```

# Preparing data in cvs format
```python
def txt_to_csv(file_path):
    """ convert .txt files to .csv format and prepare data
    :param file_path: list of paths type:string
    :return:
    """
    for file in file_path:
        df_txt = pd.read_csv(file, sep="\t")

        df_date = df_txt['Event Date'].unique()
        df_date.sort()

        url = f'https://api.exchangerate.host/timeseries?start_date={df_date[0]}&end_date={df_date[-1]}'
        response = requests.get(url)
        data = response.json()

        df_rates_by_date = pd.DataFrame(data["rates"])

        df_proceeds_usd = pd.Series([], dtype='float64')

        for i, row in df_txt.iterrows():
            if row[13] == 'EUR':
                proceeds_usd = row[12] * df_rates_by_date.loc['USD', row[0]]
            elif row[13] == 'USD':
                proceeds_usd = row[12]
            else:
                proceeds_usd = (row[12] / df_rates_by_date.loc[row[13], row[0]]) * df_rates_by_date.loc[
                    'USD', row[0]]
            df_proceeds_usd = pd.concat([df_proceeds_usd, pd.Series(float('{:.2f}'.format(proceeds_usd)))],
                                        ignore_index=True)

        df_txt.insert(14, 'Proceeds USD', df_proceeds_usd)

        df_txt.to_csv(file[:-4] + '.csv', index=None)
```


# Function which UPLOAD data from reports to DATABASE
```python
def add_to_database(connection, table, list_of_paths):
    """ add data to the table
    :param connection: Connection object
    :param table: a TABLE to add
    :param list_of_paths: list of paths to data files
    :return:
    """
    for path in list_of_paths:
        try:
            with open(path, 'r') as f:
                reader = csv.reader(f)
                columns = next(reader)
                query = 'INSERT OR IGNORE INTO {0} ({1}) VALUES ({2})'
                query = query.format(table, ','.join(['hash_row',
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
                        print(e)

        except Exception as e:
            print(e)
```
