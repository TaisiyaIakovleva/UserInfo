import requests
from requests.auth import HTTPBasicAuth
from requests import HTTPError
import json
import pandas as pd
import numpy as np
import configparser
from sqlalchemy.sql.functions import user
from connect import Connect
from sqlalchemy import create_engine, text
import gspread
import pygsheets
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import logging

class UsersInfo:

    def __init__(self, user_id):
        if type(user_id) == str:
            self.flag = str
        else:
            self.flag = list
        self.login = user_id

    def get_accounts_info(self, table_type):
        config = configparser.ConfigParser()
        config.read("/Users/Taisia1/Desktop/octacode/config.ini")
        domain = config["userInfo"]["domain"]
        login = config["userInfo"]["login"]
        password = config["userInfo"]["passwd"]
        url_get = config["userInfo"]["url_get"]
        if self.flag == list:
            df = pd.DataFrame()
            for up in self.login:
                logging.basicConfig(level=logging.INFO, filename="py_log.log", filemode="w",
                                    format="%(filename)s %(asctime)s %(levelname)s %(message)s")
                try:

                    response = requests.get(
                            f'{url_get}/{domain}/{up}',
                        auth=HTTPBasicAuth(login, password))
                    response_code = response
                    response = response.json()
                    logging.info(f"get info successful with result: {response_code.status_code}.")
                    print(response_code.status_code)
                except requests.exceptions.RequestException as err:
                    logging.error(f"requests.exceptions.RequestException: {response_code.status_code}", exc_info=True)
                    print(response_code.status_code)
                    raise SystemExit(err)
                new_df = pd.DataFrame(response['accounts'])
                new_df['user_id'] = up
                df = pd.concat([df, new_df], ignore_index=True)
        else:
            logging.basicConfig(level=logging.INFO, filename="py_log.log", filemode="w",
                                format="%(filename)s %(asctime)s %(levelname)s %(message)s")
            try:
                response = requests.get(f'{url_get}/{domain}/{self.login}',
                             auth=HTTPBasicAuth(login, password))
                response_code = response
                response = response.json()
                logging.info(f"get info successful with result: {response_code.status_code}.")
                print(response_code.status_code)
            except requests.exceptions.RequestException as err:
                logging.error(f"requests.exceptions.RequestException: {response_code.status_code}", exc_info=True)
                print(response_code.status_code)
                raise SystemExit(err)
            df = pd.DataFrame(response['accounts'])
            df['user_id'] = self.login
        categories = pd.DataFrame()
        for row in range(len(df)):
            categories = pd.concat([categories,
                                    pd.DataFrame(df['categories'][row]).pivot_table(columns='category', values='value',
                                                                                    aggfunc=lambda x: ' '.join(x))],
                                   axis=0)
        df = pd.concat([df.drop(columns='categories'), categories.reset_index()], axis=1)
        df = df.loc[df['clearingCode'] == 'LIVE']
        req = f"""
        SELECT event_symbol as currency, snapshot_time, bid_price as price
        FROM ( SELECT event_symbol, snapshot_time, bid_price, ROW_NUMBER() OVER (PARTITION BY event_symbol ORDER BY snapshot_time DESC) 
        as row_num FROM dxcore.dxcore.quotes_history ) as subquery
        WHERE row_num = 1 and event_symbol in ('BTC/USD', 'ETH/USD')"""
        conn = Connect()
        engine = conn.connect_api_devex()
        with engine.connect() as con:
            market_data = pd.DataFrame(con.execute(text(req)))
        market_data['currency'] = market_data['currency'].str.partition('/')[0]
        df = df.merge(market_data[['currency', 'price']], how='left', on=['currency'])
        df['price'] = df['price'].fillna(1)
        df['balance_usd'] = (df['balance'].astype(float) * df['price'].astype(float)).round(2)
        df = df.drop(columns={'type', 'brokerCode', 'accountCashType', 'accountType', 'index', 'price'})
        if table_type:
            return df[['user_id', 'accountCode', 'currency', 'balance_usd', 'AutoExecution', 'Margining']]
        else:
            return df

    def google_sheets(self, table_type):
        df = user.get_accounts_info(table_type)
        key = "/Users/Taisia1/Desktop/octacode/deposite/creds.json"
        client = pygsheets.authorize(service_file=key)
        sh = client.open("order_statistics")
        wks = sh.worksheet_by_title('UserInfo')
        wks.clear()
        wks.set_dataframe(
            df, (1, 1),
            copy_index=False, header=True
        )

    def change_value(self, accountCode, categoryCode, value):
        config = configparser.ConfigParser()
        config.read("/Users/Taisia1/Desktop/octacode/config.ini")
        login = config["userInfo"]["login"]
        password = config["userInfo"]["passwd"]
        url_put = config["userInfo"]["url_put"]
        logging.basicConfig(level=logging.INFO, filename="py_log.log", filemode="w",
                            format="%(asctime)s %(levelname)s %(message)s")
        try:
            response = requests.put(
                f'{url_put}/{accountCode}/category/{categoryCode}/',
                json={"value": f"{value}"}, auth=HTTPBasicAuth(login, password))
            response = response.json()
            logging.info(f"change_value successful with result: {response.status_code}.")
            print(response.status_code)
        except requests.exceptions.RequestException as err:
            logging.error(f"requests.exceptions.RequestException: {response.status_code}", exc_info=True)
            print(response.status_code)
            raise SystemExit(err)

user_id = 'ud632544530'
user = UsersInfo(user_id)

b = (user.get_accounts_info(1))
a = (user.change_value("BTC_ud632544530_1", "Margining", "SVT_Leverage_5"))
print(b)
pass