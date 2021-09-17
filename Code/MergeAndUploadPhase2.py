"""
@author = "TeamPhoenixMiners"
@version = 0.1
"""
import pandas as pd
import xlrd
import sqlalchemy
import mysql.connector

"""
MERGE class contains all methods to get data, merge it and transfer it to a MySQL DB instance. Credentials for MySQL 
instance are set default as 'root' for username and 'password' for password intentionally as a measure of good 
security practice. 
"""
class MERGE:

    def __init__(self):
        """
        Contains globally used data containers initialized as Nine
        """
        self.combined_data = None
        self.lapd_sample = None
        self.nypd_sample = None

    def get_data(self):
        """
        Retrieves sample data
        """
        # self.nypd_sample = pd.read_excel("/Users/nutxa36/PycharmProjects/untitled/Data/nypd_sample_raw.xlsx")
        # self.lapd_sample = pd.read_excel("/Users/nutxa36/PycharmProjects/untitled/Data/lapd_sample_raw.xlsx")
        self.cleaned_combined_data = pd.read_csv("/Users/nutxa36/PycharmProjects/PhoenixMinersPhase3/Data/cleaned_data_100k.csv")

    def merge_data(self):
        """
        Concatenate data from different sources modified to contain similar columns manually
        """
        self.combined_data = pd.concat([self.nypd_sample, self.lapd_sample], ignore_index=True)

    def merged_data_to_csv(self):
        """
        Store merged data in a csv file
        """
        self.combined_data.to_csv("/Users/nutxa36/PycharmProjects/untitled/Data/raw_combined_data.csv")

    def check_columns(self):
        """
        Check whether different datasets have similar columns
        """
        nypd_columns = self.nypd_sample.columns
        lapd_columns = self.lapd_sample.columns

        for x in range(len(nypd_columns)):
            if nypd_columns[x] == lapd_columns[x]:
                print(nypd_columns[x], lapd_columns[x])

    def combined_data_to_mysqldb(self):
        """
        Upload merged data to a MySQL Database instance
        """
        # mysql connection credentials
        username = 'root'
        password = 'password'
        ip_address = '127.0.0.1'
        schema = 'CrimeData'
        # start a connection with MySQL DB instance by providing connection credentials and initializing a connection
        # request
        conn = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                       format(username, password,
                                                              ip_address, schema,
                                                              auth_plugin='mysql_native_password'))
        # transfer data to table named 'Crime Data' with the help of connection established in connection instance
        # 'conn'. If a table with same name exists, replace it
        self.cleaned_combined_data.to_sql(con=conn, name='Crime Data', if_exists='replace')


if __name__ == '__main__':
    MergeObject = MERGE()
    MergeObject.get_data()
    # MergeObject.merge_data()
    # MergeObject.merged_data_to_csv()
    MergeObject.combined_data_to_mysqldb()
