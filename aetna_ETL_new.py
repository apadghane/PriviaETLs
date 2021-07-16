import os
import json
import pandas
import dotenv
import datetime
import sqlalchemy


class Database:
    def __init__(self):
        dotenv.load_dotenv()

        self.__db_url__ = f'mssql+pyodbc://{os.environ["SERVER_NAME"]}/{os.environ["DATABASE"]}?driver={os.environ["DRIVER"]}'
        self.__engine__ = sqlalchemy.create_engine(self.__db_url__)

    def load_to_table(self, df: pandas.DataFrame = None, table: str = None):
        df.to_sql(table, self.__engine__, if_exists="append", index=False)


class AetnaETL:
    def __init__(self):
        self.__config__ = json.loads(open("config.json").read())["aetna"]

        self.__data_list__ = None
        self.__run_time__ = datetime.datetime.now()

        self.__database__ = Database()

    def __get_files__(self) -> list:
        return os.listdir(self.__config__["data"])

    def __file_wise_transform__(self, file: str = None):
        if "MEMBERSHIPREPORT" in file:
            market = self.__config__["markets"][file.split("_")[1].lower()]
            file_date = datetime.datetime.strptime(file.split("_")[-1].split(".")[0], "%Y%m").replace(day=1)

            df = pandas.read_excel(os.path.join(self.__config__["data"], file), sheet_name=self.__config__["sheet"],
                                   names=self.__config__["columns"], header=None)

            df = df.iloc[7:, :]

            df["Market"] = market
            df["File_Name"] = file
            df["File_Date"] = file_date
            df["Updated_Date"] = self.__run_time__

            return df

    def __file_wise_load__(self, df: pandas.DataFrame = None):
        self.__database__.load_to_table(df=df, table=self.__config__["table"])

    def extract_transform(self):
        files = self.__get_files__()
        self.__data_list__ = list(map(self.__file_wise_transform__, files))

    def load(self):
        list(map(self.__file_wise_load__, self.__data_list__))


if __name__ == "__main__":
    etl = AetnaETL()

    etl.extract_transform()

    etl.load()
