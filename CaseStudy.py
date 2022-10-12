#Eshaan Vora
#Case Study: Data Solutions Consultant
#Solutions Engineering Team
#04/06/2022

#Note: We will create a database named "DataSolutions_CaseStudy" in our MySQL Server Local Connection, if this database does not already exist

#We will clean, format and then import legacy data into the newly created "DataSolutions_CaseStudy" database
#and then query this database to transform the legacy data to the required usable format

# Import packages
import os
import matplotlib.pyplot as plt
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import pymysql
import csv
import mysql.connector
from mysql.connector import errorcode

# Import data from .csv file into Pandas Dataframe
fname = "FilesReceived_LegacyData/AccountTransactions.csv"
transactions_df = pd.read_csv(fname)
fname = "ReferenceFiles/TransactionTypeMappings.csv"
reference_transactions_df = pd.read_csv(fname)
fname = "FilesReceived_LegacyData/PositionDetails.csv"
position_details_df = pd.read_csv(fname)
fname = "ReferenceFiles/SecurityTypeMappings.csv"
reference_security_mapping_df = pd.read_csv(fname)
fname = "FilesReceived_LegacyData/PositionQuantity.csv"
quantity_df = pd.read_csv(fname)
fname = "FilesReceived_LegacyData/PositionValues.csv"
values_df = pd.read_csv(fname)

#Remove spaces in the column names of the dataframe
transactions_df.columns = transactions_df.columns.str.replace(' ','_')
reference_transactions_df.columns = reference_transactions_df.columns.str.replace(' ','_')
position_details_df.columns = position_details_df.columns.str.replace(' ','_')
reference_security_mapping_df.columns = reference_security_mapping_df.columns.str.replace(' ','_')
quantity_df.columns = quantity_df.columns.str.replace(' ','_')
values_df.columns = values_df.columns.str.replace(' ','_')


##########################################
#CREATE STAGING TABLE TO CALCULATE PRICE PER SHARE PER DAY
#To derive price-per-share values per day, Postion Values are divided by Position Quantity (the number of shares)

#Postion Values are found in the 'PositionValues.csv' file
#Position Quantities are found in the 'PositionQuantity.csv' file

#results[] will store the Position, Date, and Price Per Share as a list of lists for the most efficient storage of result calculations
results = []
#dateCounter will extract the date while iterating through the each day of position quantities and values
dateCounter = 0

#Iterate through columns
for i in range(1,len(quantity_df.columns)):
    dateCounter += 1
    date = quantity_df.columns[dateCounter]
    #Format 'date' string to the appropriate format
    date = date[date.find("(")+1:date.find(")")]
    #Iterate through rows
    for y in range(0,len(quantity_df)):
        if quantity_df.iloc[y, i] != 0:
            results.append([quantity_df.iloc[y,0], date, values_df.iloc[y,i]/quantity_df.iloc[y,i]])
        else:
            results.append([quantity_df.iloc[y,0], date, 0])

#Write results to CSV to later be able to quickly create a Pandas Dataframe and upload to a MySQL staging table
stagingFile = 'EXPORT_BIN/stagingTable.csv'
with open(stagingFile, 'w') as csvfile:
    writeFile = csv.writer(csvfile)
    writeFile.writerow(['Position','Date','Price_Per_Share'])
    for row in results:
        writeFile.writerow(row)
    csvfile.close()

price_per_share_df = pd.read_csv(stagingFile)
os.remove(stagingFile)

###########################################
#Set credentials for Database connection
hostName = "localhost"
dbName = "DataSolutions_CaseStudy"
userName = "root"
password = "Password"

#Create SQL Connection to upload data into MySQL Server
sqlEngine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
    .format(host=hostName, db=dbName, user=userName, pw=password))
if not database_exists(sqlEngine.url):
    create_database(sqlEngine.url)
dbConnection = sqlEngine.connect()
###########################################

#Create MySQL tables from the loaded dataframes, if the tables do not already exist
try:
    transactions_df.to_sql("Transactions", dbConnection, if_exists='fail')
    reference_transactions_df.to_sql("TransactionTypeMappings", dbConnection, if_exists='fail')
    position_details_df.to_sql("PositionDetails", dbConnection, if_exists='fail')
    reference_security_mapping_df.to_sql("SecurityTypeMappings", dbConnection, if_exists='fail')
    quantity_df.to_sql("Quantities", dbConnection, if_exists='fail')
    values_df.to_sql("Values", dbConnection, if_exists='fail')
    price_per_share_df.to_sql("PricePerShare", dbConnection, if_exists='fail')

except Exception as ex:
    print(ex)
else:
    print("Tables created successfully")


def write_csv_prompt(results,columnNames,fileName,userInput):
    while userInput.isdigit() == True:
        print("Incorrect option. Try again")
        userInput = input("Enter choice (y/n): ")
    if userInput[0].upper() == "Y":
        filePath = "EXPORT_BIN/" + fileName + ".csv"
        with open(filePath, 'w') as csvfile:
            writeFile = csv.writer(csvfile)
            writeFile.writerow(columnNames)
            writeFile.writerows(results)
            csvfile.close()

TransactionsImport = """SELECT Security as Owned_Name, temp.New_Security_Type as Owned_Type,
TransactionTypeMappings.New_Transaction_Type as Type,
Trade_Date as Date, Quantity as Units, Value as Amount, Currency
FROM Transactions
INNER JOIN TransactionTypeMappings
ON Transactions.Transaction_Type = TransactionTypeMappings.Transaction_Type
INNER JOIN (
SELECT PositionDetails.CUSIP, SecurityTypeMappings.New_Security_Type
FROM PositionDetails
INNER JOIN SecurityTypeMappings
ON PositionDetails.Asset_Type = SecurityTypeMappings.Security_Type) temp
ON Transactions.CUSIP = temp.CUSIP;"""

HistoricalPricesImport = """SELECT PricePerShare.Position as Owned_Name, SecurityTypeMappings.New_Security_Type as Owned_Type,
Date, Price_Per_Share as Price, PositionDetails.Currency as Currency,
PositionDetails.Principal_Factor as Principal_Factor
FROM PricePerShare
INNER JOIN PositionDetails
ON PricePerShare.Position = PositionDetails.Position
INNER JOIN SecurityTypeMappings
ON PositionDetails.Asset_Type = SecurityTypeMappings.Security_Type;"""


results1 = dbConnection.execute(TransactionsImport)
columnNames1 = dbConnection.execute(TransactionsImport).keys()
results2 = dbConnection.execute(HistoricalPricesImport)
columnNames2 = dbConnection.execute(HistoricalPricesImport).keys()

choice = input("Save results to CSV? (Y/N) ")

write_csv_prompt(results1,columnNames1,"Transactions_Import_Results",choice)
write_csv_prompt(results2,columnNames2,"HistoricalPrices_Import_Results",choice)

dbConnection.close()
