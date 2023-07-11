import requests
from requests.auth import HTTPBasicAuth
import json
import pandas as pd
import numpy as np
import configparser  # импортируем библиотеку
from connect import Connect
from sqlalchemy import create_engine, text
import gspread
import pygsheets
from gspread_dataframe import get_as_dataframe, set_with_dataframe


class UsersInfo():

    def __init__(self, user_id):
        if type(user_id) == str:
            self.flag = str
        else:
            self.flag = list
        self.login = user_id

    def get_accounts_info(self, table_type):

        config = configparser.ConfigParser()  # создаём объекта парсера
        config.read("/Users/Taisia1/Desktop/octacode/deposite/config.ini")  # читаем конфиг
        domain = config["userInfo"]["domain"]
        login = config["userInfo"]["login"]
        password = config["userInfo"]["passwd"]
        url = config["userInfo"]["url"]
        if self.flag == list:
            df = pd.DataFrame()
            for up in user_id:
                r = json.dumps(
                    requests.get(
                        f'{url}/{domain}/{up}',
                        auth=HTTPBasicAuth(login, password)).json())
                r = json.loads(r)
                new_df = pd.DataFrame(r['accounts'])
                new_df['user_id'] = up
                df = pd.concat([df, new_df], ignore_index=True)

        else:
            r = json.dumps(
                requests.get(f'{url}/{domain}/{self.login}',
                             auth=HTTPBasicAuth(login, password)).json())
            r = json.loads(r)
            df = pd.DataFrame(r['accounts'])

        categories = pd.DataFrame()
        for row in range(len(df)):
            categories = pd.concat([categories,
                                    pd.DataFrame(df['categories'][row]).pivot_table(columns='category', values='value',
                                                                                    aggfunc=lambda x: ' '.join(x))], axis=0)
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
        market_data['currency'] = (market_data['currency'].astype('str'))
        market_data['currency'] = market_data['currency'].str.partition('/')[0]
        df = df.merge(market_data[['currency', 'price']], how='left', on=['currency'])
        df['price'] = df['price'].fillna(1)
        df['balance_usd'] = (df['balance'].astype(float) * df['price'].astype(float)).round(2)
        df = df.drop(columns={'type', 'brokerCode', 'accountCashType', 'accountType', 'index', 'price'})
        if table_type == True:
            return df[['user_id', 'accountCode', 'currency', 'balance_usd', 'AutoExecution', 'Margining']]
        else:
            return df

    def google_sheets(self, table_type):
        df = user.get_accounts_info(table_type)
        key = "/Users/Taisia1/Desktop/octacode/deposite/creds.json"
        client = pygsheets.authorize(service_file=key)
        sh = client.open("order_statistics")
        wks = sh.worksheet_by_title('UserInfo')
        wks.set_dataframe(
            df, (1, 1),
            copy_index=False, header=True
        )


user_id = ['ud632544530', 'ud632544530']
user = UsersInfo(user_id)
a = (user.google_sheets(1))
pass
