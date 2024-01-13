import os
from azure_strg_utils import AzureStorageUtils
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
CONNECTION_STRING=os.getenv('CONNECTION_STRING')

client=AzureStorageUtils(connection_string=CONNECTION_STRING)

# # list containers
container=client.list_container()
print(container)

# # # list folders 
# folder=client.list_blobs(container_name='rawdata')
# print(folder)

# # list files which is present inside a folder
# folder_files=client.list_files(container_name='rawdata',blob_name='raw')
# print(folder_files)

#folder_files=client.conditional_operation(container_name='rawdata',blob_name='raw',creation_date='2023-12-15',comparison='greater_than',action='download',file_regex='c*')
#folder_files=client.conditional_operation(container_name='rawdata',blob_name='raw',creation_date='2023-12-15',comparison='greater_than',action='download')

#client.copy_blob(container_name='rawdata',blob_name='raw',destination_container='new-test',destination_blob='raw',file_name='Non_sales_data.csv')
#client.copy_blob(container_name='rawdata',blob_name='raw',destination_container='new-test',destination_blob='raw',all_files=True,file_regex='c*')
# status=client.copy_blob(container_name='rawdata',blob_name='raw',destination_container='rawdata',destination_blob='test',all_files=True)
# print(type(status))
#client.copy_blob(container_name='rawdata',blob_name='raw',destination_container='new-test',destination_blob='raw',creation_date='2023-12-15',comparison='greater_than',file_regex='c*')

# get pandas dataframe of a file present inside a folder
df=client.download_file(container_name='rawdata',blob_name='raw',file_name='cars_new.csv',is_dataframe=True)
print(df)

# # download specified file in specified folder
#client.download_file(container_name='rawdata',blob_name='test',file_name='sales_data.csv',path='./test')

# # download all file which have name starting from 'cust'
#client.download_file(container_name='rawdata',blob_name='raw',path='downloaded',all_files=True)

# client.download_file(container_name='rawdata',blob_name='multi',path='multi',all_files=True)

# # upload single file from specified path file_path into blob/folder raw 
#client.upload_file(container_name='rawdata',blob_name='test',file_name='cars_new.csv',file_path='data')

# upload all files which have name 'cust*' from specified folder to specified blob
# client.upload_file(container_name='rawdata',blob_name='multi/raw',all_files=True,file_path='downloaded/raw',file_regex='cust*')

# # upload all file present inside specified folder
# file_status=client.upload_file(container_name='rawdata',blob_name='test_new',all_files=True,file_path='TestCases/test/raw')
# print(file_status)

result=client.upload_dataframe(dataframe=df,file_name='cars.xml',container_name='rawdata',blob_name='dataframe')
print(result)
# # delete all files present inside a blob
#client.delete_file(container_name='rawdata',blob_name='raw',all_files=True)

# # delete all files present inside a blob which have name 
# client.delete_file(container_name='rawdata',blob_name='raw',all_files=True,file_regex='cust*')

# # delete single files present inside a blob
# client.delete_file(container_name='rawdata',blob_name='raw',file_name='Product_data.csv')

# # create a container
#client.create_container(container_name='test2')

# # deleate a container
# client.delete_container(container_name='test')

# # deleate all containers
# client.delete_container(all_containers=True)
