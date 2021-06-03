from azure.storage.blob import BlobServiceClient
from azure.storage.blob import ContainerClient
from azure.storage.blob import BlobClient
import requests
import json
import pandas as pd
import datetime
import os
import cmd
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
import apisecrets


# Set credentials (import from keyvault) -----------------------------------------------------------------------
keyVaultName = 'metro-servicenow'
KVUri = f"https://{keyVaultName}.vault.azure.net"
azure_credential = DefaultAzureCredential()
client = SecretClient(vault_url=KVUri, credential=azure_credential)
secretName = apisecrets.secretName
secret = client.get_secret(secretName)

user = apisecrets.user
pwd = secret.value


# Azure container connection string ------------------------------------------------------------------------------
# Blob client
blob_credential = apisecrets.blob_credential
service = BlobServiceClient(account_url="https://metrosnowblob.blob.core.windows.net/",
    credential=blob_credential)

# Container connection string
connection_string = apisecrets.connection_string

# Container client:
container_client = ContainerClient.from_connection_string(conn_str=connection_string,
                                                          container_name="does-this-work")


# Blob update function  --------------------------------------------------------------------------------------------
def update_backlog_blob(url, type, username=user, password=pwd):
    headers = {"Content-Type":"application/json","Accept":"application/json"}
    response = requests.get(url, auth=(username, password), headers=headers )
    if response.status_code != 200: 
        print('Status:', response.status_code, 'Headers:', response.headers, 'Error Response:',response.json())
        exit()

    data = response.json()
    df_main = pd.DataFrame.from_dict(data['result'])
    euc_daily_count = str(df_main['state'].count())

    # Updating EUC blob ---------------------------------------------------------------------------------------------
    # EUC blob connection string
    blob = BlobClient.from_connection_string(conn_str=connection_string,
                                            container_name="does-this-work",
                                            blob_name=f"backlog_data_{type}.csv")

    # check to see if euc blob exists
    if blob.exists():
        # if it does exist, append a row to it
        # read in the current blob contents as a df
        myfile = rf"./backlog_data_{type}.csv"
        with open(myfile, "wb") as my_blob:
            blob_data = blob.download_blob()
            blob_data.readinto(my_blob)
        dataframe_blobdata = pd.read_csv(myfile)
        dataframe_blobdata

        # create a new df so that it can be appended to the first
        time = str(datetime.datetime.now())
        d = {'inc_count': [euc_daily_count], 'time': [time], 'time_copy': [time]}
        df2 = pd.DataFrame(d)
        df2.set_index('time_copy', inplace=True)
        df2

        # append and overwrite existing df
        dataframe_blobdata = dataframe_blobdata.append(df2)

        # upload the updated csv to the azure container
        dataframe_blobdata.to_csv(f'backlog_data_{type}.csv', index=False)
        myfile = rf"./backlog_data_{type}.csv"

        # Upload the file
        with open(myfile, "rb") as data:
            blob.upload_blob(data, overwrite=True)

    # if blob does not exist, create it with an instance count of 0 and current time
    else:
        # create new df with count=0 and time=current_time
        data = [[0, datetime.datetime.now(), datetime.datetime.now()]] 
        df = pd.DataFrame(data, columns = ['inc_count', 'time', 'time_copy'])
        df.set_index('time_copy', inplace=True)

        # write df out to a csv
        df.to_csv(f'backlog_data_{type}.csv', index=False)
        myfile = rf"./backlog_data_{type}.csv"
        
        # Upload the file to the container
        with open(myfile, "rb") as data:
            blob.upload_blob(data, overwrite=True)