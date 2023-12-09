import os
from AzureBlobUtils.AzureUtils import AzureStorageUtils


CONNECTION_STRING=""
client=AzureStorageUtils(connection_string=CONNECTION_STRING)

# # list containers
# container=client.list_container()
# print(container)

# # list folders 
# folder=client.list_folders(container_name='rawdata')
# print(folder)

# # list files which is present inside a folder
# blob,folder_files=client.list_files(container_name='rawdata',blob_name='raw')
# print(folder_files)

# get pandas dataframe of a file present inside a folder
# df=client.download_file(container_name='rawdata',blob_name='raw',file_name='cars_new.csv',is_dataframe=True)
# print(df)

# # download specified file in specified folder
# client.download_file(container_name='rawdata',blob_name='raw',file_name='sales_data.csv',path='./test')

# # download all file which have name starting from 'cust'
# client.download_file(container_name='rawdata',blob_name='raw',path='downloaded',all_files=True)

# client.download_file(container_name='rawdata',blob_name='multi',path='multi',all_files=True)

# # upload single file from specified path file_path into blob/folder raw 
# client.upload_file(container_name='rawdata',blob_name='raw',file_name='Product_data.csv',file_path='downloaded/raw')

# upload all files which have name 'cust*' from specified folder to specified blob
# client.upload_file(container_name='rawdata',blob_name='multi/raw',all_files=True,file_path='downloaded/raw',file_regex='cust*')

# # upload all file present inside specified folder
# client.upload_file(container_name='rawdata',blob_name='raw',all_files=True,file_path='Data_Profiling/data')

# # delete all files present inside a blob
# client.delete_file(container_name='rawdata',blob_name='raw',all_files=True)

# # delete all files present inside a blob which have name 
# client.delete_file(container_name='rawdata',blob_name='raw',all_files=True,file_regex='cust*')

# # delete single files present inside a blob
# client.delete_file(container_name='rawdata',blob_name='raw',file_name='Product_data.csv')

# # create a container
# client.create_container(container_name='test')

# # deleate a container
# client.delete_container(container_name='test')

# # deleate all containers
# client.delete_container(all_containers=True)
