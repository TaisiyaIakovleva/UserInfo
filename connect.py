import configparser
from sqlalchemy import create_engine


class Connect:

    def connect_api_devex(self):
        config = configparser.ConfigParser()
        config.read("/Users/Taisia1/Desktop/octacode/config.ini")
        username = config["dxcore"]["username"]
        passwd = config["dxcore"]["passwd"]
        host = config["dxcore"]["host"]
        db = config["dxcore"]["db"]
        try:
            engine = create_engine(f"postgresql://{username}:{passwd}@{host}/{db}")
            print('connection success')
        except Exception as err:
            print(err)
        return engine

