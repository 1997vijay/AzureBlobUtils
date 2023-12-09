# AzureBlobUtils
<<<<<<< HEAD
[![PyPI version](https://badge.fury.io/py/your-package-name.svg)](https://badge.fury.io/py/your-package-name)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python Versions](https://img.shields.io/pypi/pyversions/your-package-name.svg)](https://pypi.org/project/your-package-name/)
[![Downloads](https://pepy.tech/badge/your-package-name)](https://pepy.tech/project/your-package-name)

Package can be used to interact with azure storage account to perform various activities.

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install foobar.

```bash
pip install AzureBlobUtils
```

## Usage

```python
from AzureBlobUtils import AzureStorageUtils
```
# Create connection
```python
# provide the connection string
CONNECTION_STRING=os.getenv('CONNECTION_STRING')
client=AzureStorageUtils(connection_string=CONNECTION_STRING)
```

# Returns list of containers
```python
# list containers
container=client.list_container()
print(container)
```

# Create a container name 'test'
```python
# create a container
client.create_container(container_name='test')
```

# Returns list of folders
```python
# list folders 
folder=client.list_folders(container_name='test')
print(folder)
```
# Returns list of files
```python
# list files which is present inside a blob/folder
blob,folder_files=client.list_files(container_name='test',blob_name='raw')
print(folder_files)
```

# Returns pandas dataframe of a file. Use is_dataframe=True .
```python
# get pandas dataframe of a file present inside a folder
df=client.download_file(container_name='test',blob_name='raw',file_name='cars.csv',is_dataframe=True)
```

# Download specified file in specific folder. pass the path using "path",
```python
# download the file in specified folder
client.download_file(container_name='test',blob_name='raw',file_name='sales_data.csv',path='./test')
```

# Download all file which have name starting from 'cust'. pass the path using "path",
```python
# download all file which have name starting from 'cust'
client.download_file(container_name='test',blob_name='raw',path='downloaded',all_files=True,file_regex='cust*.csv')
```

# Upload single file from specified path file_path into blob/folder,
```python
# upload single file from specified path file_path into blob/folder raw 
client.upload_file(container_name='test',blob_name='raw',file_name='Product_data.csv',file_path='Data_Profiling/data')
```

# Upload all file from specified path file_path into blob/folder. Use all_files=True.
```python
# upload all file present inside specified folder
client.upload_file(container_name='test',blob_name='raw',all_files=True,file_path='Data_Profiling/data')
```

# Upload all file which have name 'cust*' from specified path file_path into blob/folder. Use all_files=True and file_regex='cust*' to pass the file pattern.
```python
# upload all files which have name 'cust*' from specified folder to specified blob
client.upload_file(container_name='test',blob_name='multi/raw',all_files=True,file_path='data',file_regex='cust*')
```

# Upload all file present inside specified folder.
```python
# upload all file present inside specified folder
client.upload_file(container_name='test',blob_name='raw',all_files=True,file_path='Data_Profiling/data')
```

# Delete all files present inside a blob. Use all_files=True.
```python
# delete all files present inside a blob
client.delete_file(container_name='test',blob_name='raw',all_files=True)
```

# Delete all files present inside a blob which have name "cust*". Use all_files=True and file_regex='cust*' to pass the file pattern.
```python
# delete all files present inside a blob which have name 
client.delete_file(container_name='test',blob_name='raw',all_files=True,file_regex='cust*')
```

# Delete single files present inside a blob.
```python
# delete single files present inside a blob
client.delete_file(container_name='test',blob_name='raw',file_name='Product_data.csv')
```

# Delete a container name 'test'
```python
# delete a container
client.delete_container(container_name='test')
```

# Delete all containers from the storage account.
```python
# deleate all containers
client.delete_container(all_containers=True)
```

## Contributing
You are free to download,modify and use the code as you want.
## License
=======

[![PyPI version](https://badge.fury.io/py/azure-blob-utils.svg)](https://badge.fury.io/py/azure-blob-utils)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python Versions](https://img.shields.io/pypi/pyversions/your-package-name.svg)](https://pypi.org/project/azure-blob-utils/)

A brief description of what your package does and its main features.

## Installation

Install azure-blob-utils from PyPI (recommended) using pip:

```bash
pip install azure-blob-utils
```

# Usage
# Package is under development
>>>>>>> 53de0ecd576cf8adea512a0ad583e2dc4b0c20c1
