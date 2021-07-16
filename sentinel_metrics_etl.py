"""
                SENTINEL METRICS ETL
                author: Abhishek Padghane
"""

# importing required libraries
import os
import re
import ssl
import json
import shutil
import dotenv
import pandas
import smtplib
import datetime
import sqlalchemy
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


class Config:
    def __init__(self):
        self.__config__ = json.loads(open("config.json").read())["sentinel_metrics"]

    def get_config(self):
        return self.__config__


class Logger:
    def __init__(self, message: str = None, mode: str = None):
        self.__config__ = Config()
        self.__config__ = self.__config__.get_config()

        # initializing log message
        log_message = f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}: {message}"

        # displaying log message
        print(log_message)

        # checking if logs folder is present, if not than creating it
        if not os.path.exists(self.__config__["log"]):
            os.mkdir(self.__config__["log"])

        # writing log message in log file
        with open(self.__config__[mode], "a") as file:
            file.writelines(log_message + "\n")


class Email:
    def __init__(self):
        self.load_env_variables()

        self.__context__ = ssl.SSLContext(ssl.PROTOCOL_TLS)
        self.__mime_message__ = MIMEMultipart("alternative")

        self.__server__ = os.environ["EMAIL_SERVER"]
        self.__port__ = os.environ["EMAIL_PORT"]
        self.__sender__ = os.environ["EMAIL_SENDER"]
        self.__pass__ = os.environ["EMAIL_PASS"]

    @staticmethod
    def load_env_variables():
        # loading environment variables
        dotenv.load_dotenv()

    def send_email(self, subject: str = None, message: str = None, recipient: str = None):
        with smtplib.SMTP(self.__server__, self.__port__) as server:
            server.ehlo()
            server.starttls(context=self.__context__)
            server.ehlo()

            self.__mime_message__["Subject"] = subject
            self.__mime_message__["From"] = self.__sender__
            self.__mime_message__["To"] = recipient

            text = MIMEText(message, "plain")
            self.__mime_message__.attach(text)

            server.login(self.__sender__, self.__pass__)
            server.sendmail(self.__sender__, recipient, self.__mime_message__.as_string())


class Notify:
    def __init__(self, subject: str = None, message: str = None, recipient: str = None):
        # initializing email object
        self.__email__ = Email()

        # sending email to recipient
        self.__email__.send_email(subject=subject, message=message, recipient=recipient)


class SentinelMetrics:
    def __init__(self):
        self.__config__ = Config()
        self.__config__ = self.__config__.get_config()

        self.__file_name__ = None
        self.__month__ = None
        self.__year__ = None
        self.__data_list__ = None
        self.__engine__ = None

    def __get_file__(self):
        # checking for data directory
        if not os.path.exists(self.__config__["data"]):
            Logger("{} directory not available, please create data directory and store data file there".format(
                self.__config__["data"]), mode="error_log")
            Notify(**self.__config__["email_alerts"]["dir_not_avail"])
            exit(0)

        # checking for files which contain our search pattern
        for files in os.listdir(self.__config__["data"]):
            if bool(re.search(r'{}*'.format(self.__config__["file"]), files)):
                return files
        Logger("There are no files which contain file name {}, please add file".format(self.__config__["file"]),
               mode="error_log")
        Notify(**self.__config__["email_alerts"]["file_not_found"])
        exit(0)

    def __extract_data_sheet__(self, sheet: str = None) -> pandas.DataFrame:
        # reading data from excel file of given sheet and returning data frame
        return pandas.read_excel(os.path.join(self.__config__['data'], self.__file_name__), sheet_name=sheet)

    def __load_data_sheet__(self, df: pandas.DataFrame = None, table: str = None):
        Logger(f"Loading data in Table: {table}", mode="run_log")

        # writing data into respected table
        df.to_sql(table, self.__engine__, if_exists='replace', index=False)

    def __add_columns_data_sheet__(self, df: pandas.DataFrame = None) -> pandas.DataFrame:
        # adding month column
        df.insert(0, "Month", datetime.datetime.strptime(self.__year__ + "_" + self.__month__,
                                                         "%Y_%B").replace(day=1))

        # adding year column
        df.insert(1, "Import Timestamp", datetime.datetime.now())

        # adding provider acronym using provider group
        if "Provider Group" in df.columns:
            df.insert(3, "Provider Acronym", df["Provider Group"].apply(lambda provider_group:
                                                                        provider_group.split("-")[1].strip()))

        # returning transformed data frame
        return df

    def extract(self):
        Logger("""
                        --------------------------------------------------------------------------------
                        |                                                                              |
                        |                                   EXTRACT                                    |
                        |                                                                              |
                        --------------------------------------------------------------------------------
        """, mode="run_log")

        # getting file name, year and month
        self.__file_name__ = self.__get_file__()
        self.__year__ = re.search(r"\d{4}", self.__file_name__).group()
        self.__month__ = self.__file_name__.split(" ")[-2]

        Logger(f'File "{self.__file_name__}" found with month: {self.__month__} and year: {self.__year__}',
               mode="run_log")

        # loading data of all sheets in provided excel file
        self.__data_list__ = list(map(lambda sheet: self.__extract_data_sheet__(sheet), self.__config__["sheets"]))
        Logger(f'Data of sheets {self.__config__["sheets"]} successfully extracted', mode="run_log")

    def transform(self):
        Logger("""
                        --------------------------------------------------------------------------------
                        |                                                                              |
                        |                                   TRANSFORM                                  |
                        |                                                                              |
                        --------------------------------------------------------------------------------
        """, mode="run_log")

        # adding new columns and making new column of all the data frames
        self.__data_list__ = list(map(self.__add_columns_data_sheet__, self.__data_list__))
        Logger("Columns 'Month' and 'Import Timestamp' added successfully to all the sheets", mode="run_log")

    def load(self):
        Logger("""
                        --------------------------------------------------------------------------------
                        |                                                                              |
                        |                                     LOAD                                     |
                        |                                                                              |
                        --------------------------------------------------------------------------------
        """, mode="run_log")

        # loading environment variables
        dotenv.load_dotenv()

        # connecting to sql server database
        db_url = f'mssql+pyodbc://{os.environ["SERVER_NAME"]}/{os.environ["DATABASE"]}?driver={os.environ["DRIVER"]}'

        self.__engine__ = sqlalchemy.create_engine(db_url)
        Logger(f'Database {os.environ["DATABASE"]} connected successfully', mode="run_log")

        # initializing df and table name mapping
        df_table_name_map_list = [{"df": df, "table": table} for df, table in zip(self.__data_list__,
                                                                                  self.__config__["tables"])]

        # loading data of data frame in the tables
        list(map(lambda df_table_map: self.__load_data_sheet__(**df_table_map), df_table_name_map_list))
        Logger(f'Data successfully loaded on tables {self.__config__["tables"]}', mode="run_log")

    def cleanup(self):
        Logger("""
                        --------------------------------------------------------------------------------
                        |                                                                              |
                        |                                    CLEANUP                                   |
                        |                                                                              |
                        --------------------------------------------------------------------------------
        """, mode="run_log")

        # checking if archive folder exists
        os.path.join(self.__config__["archive"])
        if not os.path.exists(self.__config__["archive"]):
            Logger(f'No {self.__config__["archive"]} folder detected, creating {self.__config__["archive"]} folder',
                   mode="run_log")
            os.mkdir(self.__config__["archive"])

        # adding year and month to destination folder and creating folder
        dst_folder = self.__config__["archive"] + "/" + self.__year__ + "_" + self.__month__
        if not os.path.exists(dst_folder):
            Logger(f'Making {self.__year__ + "_" + self.__month__} folder in {self.__config__["archive"]}',
                   mode="run_log")
            os.mkdir(dst_folder)

        # moving data file in archive folder
        src_file = os.path.join(self.__config__['data'], self.__file_name__)
        shutil.move(src_file, dst_folder)

        Logger(f'File {self.__file_name__} moved from {self.__config__["data"]} to {dst_folder}', mode="run_log")
        Notify(**self.__config__["email_alerts"]["etl_complete"])


# running driver code
if __name__ == "__main__":
    Logger("Sentinel Metrics ETL run STARTS", mode="run_log")

    # initializing object of sentinel metrics etl
    app = SentinelMetrics()

    # running extract
    app.extract()

    # running transform
    app.transform()

    # running load
    app.load()

    # running cleanup
    app.cleanup()

    Logger("Sentinel Metrics ETL run ENDS", mode="run_log")
