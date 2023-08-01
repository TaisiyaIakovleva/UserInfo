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
import uuid


class UsersInfo:

    logging.basicConfig(level=logging.INFO, filename="py_log.log", filemode="w",
                        format="%(filename)s %(asctime)s %(levelname)s %(message)s")

    def __init__(self, user_id):
        if type(user_id) == str:
            self.flag = str
        else:
            self.flag = list
        self.login = user_id

    def read_config(self):
        config = configparser.ConfigParser()
        config.read("/Users/Taisia1/Desktop/octacode/config.ini")
        login = config["userInfo"]["login"]
        password = config["userInfo"]["passwd"]
        return login, password

    def get_accounts_info(self, table_type):
        login, password = self.read_config()
        if self.flag == list:
            df = pd.DataFrame()
            for up in self.login:
                try:
                    response = requests.get(
                            f'https://cexstage.prosp.devexperts.com/dxweb/rest/api/register/client/default/{up}',
                        auth=HTTPBasicAuth(login, password))
                    response_code = response
                    response = response.json()
                    logging.info(f"get info: successful with result: {response_code.status_code}.")
                    print(response_code.status_code)
                except requests.exceptions.RequestException as err:
                    logging.error(f"get info: requests.exceptions.RequestException: {response_code.status_code}", exc_info=True)
                    print(response_code.status_code)
                    raise SystemExit(err)
                new_df = pd.DataFrame(response['accounts'])
                new_df['user_id'] = up
                df = pd.concat([df, new_df], ignore_index=True)
        else:
            try:
                response = requests.get(f'https://cexstage.prosp.devexperts.com/dxweb/rest/api/register/client/default/{self.login}',
                             auth=HTTPBasicAuth(login, password))
                response_code = response
                response = response.json()
                logging.info(f"get info: successful with result: {response_code.status_code}.")
                print(response_code.status_code)
            except requests.exceptions.RequestException as err:
                logging.error(f"get info: requests.exceptions.RequestException: {response_code.status_code}", exc_info=True)
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
        login, password = self.read_config()
        try:
            response = requests.put(
                f'https://cexstage.prosp.devexperts.com/dxweb/rest/api/register/account/LIVE/{accountCode}/category/{categoryCode}/',
                json={"value": f"{value}"}, auth=HTTPBasicAuth(login, password))
            response = response.json()
            logging.info(f"change_value: successful with result: {response.status_code}.")
            print(response.status_code)
        except requests.exceptions.RequestException as err:
            logging.error(f"change_value: requests.exceptions.RequestException: {response.status_code}", exc_info=True)
            print(response.status_code)
            raise SystemExit(err)

    def transfer(self, clearingCode, accountCode, currency, amount, description):
        login, password = self.read_config()
        id = uuid.uuid4()
        try:
            response = requests.put(
                f'https://cexstage.prosp.devexperts.com/dxweb/rest/api/register/account/{clearingCode}/{accountCode}/adjustment/{id}',
                json={"currency": f"{currency}", "amount": f"{amount}", "description": f"{description}"},
                auth=HTTPBasicAuth(login, password))
            if response.status_code != 201:
                raise Exception
            logging.info(f"""transfer: successful with result: {response.status_code}.
            clearingCode: "{clearingCode}",
            accountCode: "{accountCode}",
            uuid: "{id}",
            currency: "{currency}", 
            amount: "{amount}", 
            description: "{description}"
            """)
            print(response.status_code)
        except Exception as err:
            logging.error(f"transfer error: {response.status_code}", exc_info=True)
            print(response.status_code)
            raise SystemExit(err)