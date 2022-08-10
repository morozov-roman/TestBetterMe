# BetterMe Test Scrypt

import pandas as pd
import requests
import upload_data


def execute_sql_command(connection, sql_command):
    """ execute sql query and print output if exist
    :param connection: Connection object
    :param sql_command: a command to execute
    :return:
    """
    c = connection.cursor()

    c.execute(sql_command)

    connection.commit()

    row = c.fetchall()

    if row:
        print(row)
        for row in c.fetchall():
            print(row)
    else:
        print("No results")


def income_by_app_period(connection, app_id_list, start_date, end_date):
    """ execute sql query and print output if exist
    :param connection: Connection object
    :param app_id_list: list of ID of the Apps
    :param start_date: first day of the period
    :param end_date: last day of the period
    :return:
    """
    c = connection.cursor()

    income = {}

    for app_id in app_id_list:
        sql_query = f"""
                    SELECT event_date, developer_proceeds, proceeds_currency
                    FROM report
                    WHERE app_apple_id = {app_id} AND event_date BETWEEN date('{start_date}') AND date('{end_date}')
                    """

        c.execute(sql_query)

        df = pd.DataFrame(c.fetchall())

        inc_sum = 0

        url = f'https://api.exchangerate.host/timeseries?start_date={start_date}&end_date={end_date}'
        response = requests.get(url)
        data = response.json()

        df_rates_by_date = pd.DataFrame(data["rates"])

        for i, row in df.iterrows():
            if row[2] == 'EUR':
                inc_sum = inc_sum + row[1]*df_rates_by_date.loc['USD', row[0]]
            elif row[2] == 'USD':
                inc_sum = inc_sum + row[1]
            else:
                inc_sum = inc_sum + (row[1]/df_rates_by_date.loc[row[2], row[0]]) * df_rates_by_date.loc['USD', row[0]]

        income[app_id] = inc_sum

    print(income)


def main():
    conn = upload_data.connect_database(r"subscriber_report.db")

    sql_create_table = """
                        CREATE TABLE IF NOT EXISTS report(
                            hash_row TEXT NOT NULL UNIQUE,
                            event_date DATE NOT NULL,	
                            app_name TEXT NOT NULL,	
                            app_apple_ID BIGINT NOT NULL,	
                            subscription_name TEXT NOT NULL,
                            subscription_apple_ID BIGINT NOT NULL,
                            subscription_group_ID BIGINT NOT NULL,	
                            subscription_duration TEXT NOT NULL,
                            introductory_price_type TEXT,
                            introductory_price_duration TEXT,
                            marketing_opt_in_duration TEXT,	
                            customer_price DECIMAL NOT NULL,
                            customer_currency TEXT NOT NULL,	
                            developer_proceeds DECIMAL NOT NULL,	
                            proceeds_currency TEXT NOT NULL,
                            proceeds_usd DECIMAl NOT NULL,
                            preserved_pricing TEXT,	
                            proceeds_reason TEXT,
                            client TEXT,
                            device TEXT NOT NULL,	
                            country TEXT NOT NULL,	
                            subscriber_ID BIGINT NOT NULL,
                            subscriber_ID_reset TEXT,	
                            refund TEXT,	
                            purchase_date DATE,	
                            units DECIMAL NOT NULL
                        ); 
                        """

    sql_subscription_duration = """
                                SELECT DISTINCT subscription_duration
                                FROM report
                                WHERE subscription_apple_ID = ({0})
                                """

    sql_subscription_variants = """
                                SELECT DISTINCT subscription_name
                                FROM report
                                WHERE app_apple_ID = ({0})
                                """

    sql_income_calculate = """
                            SELECT app_name, 
                            SUM(proceeds_usd) AS income
                            FROM report
                            WHERE event_date BETWEEN date('{0}') AND date('{1}')
                            GROUP BY app_apple_id
                            """

    sql_calculate_conversion = """
                                WITH trial_id AS (
                                    SELECT subscriber_id, subscription_name
                                    FROM report 
                                    WHERE introductory_price_type = 'Free Trial' AND subscription_apple_id = {0} 
                                    AND event_date = DATE('{1}')
                                )
                                SELECT(SELECT COUNT(report.subscriber_id) FROM report INNER JOIN trial_id 
                                ON report.subscriber_id = trial_id.subscriber_id 
                                AND report.subscription_name = trial_id.subscription_name
                                WHERE event_date BETWEEN DATE('{1}') and DATE('{1}', '+7 day') 
                                AND report.introductory_price_type = '') AS total_trial,
                                (SELECT COUNT(trial_id.subscriber_id)
                                FROM trial_id
                                ) AS total_purchase
                                """

    # upload_data.txt_to_csv(['itunes_dataset/20190201.txt', 'itunes_dataset/20190202.txt',
    #                         'itunes_dataset/20190203.txt', 'itunes_dataset/20190204.txt',
    #                         'itunes_dataset/20190205.txt', 'itunes_dataset/20190206.txt',
    #                         'itunes_dataset/20190207.txt', 'itunes_dataset/20190208.txt',
    #                         'itunes_dataset/20190209.txt', 'itunes_dataset/20190210.txt'])

    # create/add to tables
    if conn is not None:
        # create report table
        upload_data.create_table(conn, sql_create_table)

        # add to report table
        upload_data.add_to_database(conn, 'report', ['itunes_dataset/20190201.csv',
                                                     'itunes_dataset/20190202.csv',
                                                     'itunes_dataset/20190203.csv',
                                                     'itunes_dataset/20190204.csv',
                                                     'itunes_dataset/20190205.csv',
                                                     'itunes_dataset/20190206.csv',
                                                     'itunes_dataset/20190207.csv',
                                                     'itunes_dataset/20190208.csv',
                                                     'itunes_dataset/20190209.csv',
                                                     'itunes_dataset/20190210.csv'])

        # find the subscription duration by ID
        execute_sql_command(conn, sql_subscription_duration.format(1447369566))

        # find subscription variants in the app
        execute_sql_command(conn, sql_subscription_variants.format(1398851503))

        # calculate income by app
        # income_by_app_period(conn, [1398851503, 1363010081], '2019-02-01', '2019-02-02')
        execute_sql_command(conn, sql_income_calculate.format('2019-02-01', '2019-02-02'))

        # calculate conversion from the free trial
        execute_sql_command(conn, sql_calculate_conversion.format(1447369566, '2019-02-01'))

    else:
        print("Error! cannot create the database connection.")

    conn.close()


if __name__ == '__main__':
    main()
