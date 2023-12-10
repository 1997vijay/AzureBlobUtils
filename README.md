# Package is under development, however you can download the wheel file and use it.

# AzureStorageUtils
[![PyPI version](https://badge.fury.io/py/azure_strg_utils.svg)](https://badge.fury.io/py/azure_strg_utils/0.1.1)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python Versions](https://img.shields.io/pypi/pyversions/azure_strg_utils.svg)](https://pypi.org/project/azure_strg_utils)

The package facilitates interaction with Azure storage accounts, enabling the execution of a wide range of operations on these accounts.

## Installation

Use the package manager `pip` to install azure_strg_utils.

```bash
pip install azure_strg_utils
```

## Usage
* **Create Container**: Allows the creation of containers within the storage account to organize and store data.
* **List Containers**: Provides a list of all containers present within the storage account.
* **List Blobs**: Enables the listing of blobs (files) within a container.

* **Download Operations**:
    * **Download Single File**: Retrieves a specific file from a container.
    * **Download Specific File**: Downloads a particular file from a container.
    * **Download Entire Blob**: Downloads all files present within a blob.

* **Upload Operations**:
    * **Upload Single File**: Adds a single file to a specified container.
    * **Upload Specific File**: Uploads a particular file to a container.
    * **Upload all File**: Upload all files to a specified container.

* **Delete Operations**:
    * **Delete Single File from Blob**: Removes a specific file from a blob in a container.
    * **Delete Specific File(s) Using Regex**: Deletes files matching a specific pattern from a blob.
    * **Delete Single or All Containers**: Deletes a particular container or all containers within the storage account.

* **File Regex Operations**:
Apply File Regex: Allows operations such as download, deletion, or upload based on files that match a specific pattern within a blob.
These operations provide the ability to manage containers, files (blobs), and perform various actions like downloading, uploading, and deleting specific or multiple files based on certain patterns within the Azure storage account. Please use these operations with caution, especially deletion operations, as they might result in permanent data loss.
```python
from azure_strg_utils import AzureStorageUtils
```
### Establish a connection.
Establish a connection by supplying the connection string to create an instance of AzureStorageUtils.
```python
CONNECTION_STRING=os.getenv('CONNECTION_STRING')
client=AzureStorageUtils(connection_string=CONNECTION_STRING)
```

### Get list of containers.
Produces a list of containers available within the connected Azure storage account.
This code snippet utilizes the list_container() method from the AzureStorageUtils instance (client) to retrieve and display the list of containers present in the connected Azure storage account.
```python
container=client.list_container()
print(container)
```

### Create a container.
This code snippet utilizes the create_container() method from the AzureStorageUtils to create a new container named 'test' in the connected Azure storage account.
```python
client.create_container(container_name='test')
```

### Get list of folders.
This code snippet utilizes the list_folders() method from the AzureStorageUtils to retrieve and display the list of folders within the specified container ('test') in the connected Azure storage account.
```python
folder=client.list_folders(container_name='test')
print(folder)
```

### Returns a list of files .
This code snippet utilizes the list_files() method from the AzureStorageUtils to retrieve and display the list of files within the 'raw' blob or folder, located within the 'test' container in the Azure storage account.
```python
blob,folder_files=client.list_files(
    container_name='test',
    blob_name='raw'
    )
print(folder_files)
```

### Retrieves a Pandas DataFrame.
Retrieves a Pandas DataFrame of a file located inside a folder within the Azure storage account by specifying the parameter `is_dataframe=True`.
This code snippet employs the download_file() method from the AzureStorageUtils to download and convert the 'cars.csv' file, situated within the 'raw' folder of the 'test' container in Azure storage, into a Pandas DataFrame. 
```python
df=client.download_file(
    container_name='test',
    blob_name='raw',
    file_name='cars.csv',
    is_dataframe=True
    )
```

### Downloads a specified file.
Downloads a specified file ('sales_data.csv') located in a specific folder ('raw') within the Azure storage account to a designated path using the 'path' parameter.
This code snippet utilizes the download_file() method from the AzureStorageUtils to download the 'sales_data.csv' file from the 'raw' folder in the 'test' container in Azure storage. It saves the file to the './test' directory on the local system
```python
client.download_file(
    container_name='test',
    blob_name='raw',
    file_name='sales_data.csv',
    path='./test'
    )
```

### Downloads files with specific naming pattern.
This code downloads all files from the 'raw' folder in the 'test' container that match the specified naming pattern (e.g., files with names starting with 'cust' and ending with '.csv'). Modify the file_regex parameter to match your desired naming pattern for file selection.
```python
client.download_file(
    container_name='test',
    blob_name='raw',
    path='downloaded',
    all_files=True,
    file_regex='cust*.csv'
    )
```

### Downloads all files from specific blob/folder.
This code downloads all files from the 'raw' folder in the 'test' container including the sub directory files.
```python
client.download_file(
    container_name='test',
    blob_name='raw',
    path='downloaded',
    all_files=True,
    )
```

### Uploads a single file to a specific blob/folder.
This code uses the upload_file() method from the AzureStorageUtils to upload the file 'Product_data.csv' from the 'Data_Profiling/data' directory into the 'raw' blob/folder within the 'test' container. 
```python
client.upload_file(
    container_name='test',
    blob_name='raw',
    file_name='Product_data.csv',
    file_path='Data_Profiling/data'
    )
```

### Uploads all files from a specified path to a blob/folder.
This code utilizes the upload_file() method from the AzureStorageUtils with the all_files=True parameter. It uploads all files from the 'Data_Profiling/data' directory into the 'raw' blob/folder within the 'test' container. 
```python
client.upload_file(
    container_name='test',
    blob_name='raw',
    all_files=True,
    file_path='Data_Profiling/data'
    )
```

### Uploads files with specific naming pattern from a specified path to a blob/folder.
This code utilizes the upload_file() method from the AzureStorageUtils instance (client) with the all_files=True parameter and the file_regex='cust*' parameter to upload all files matching the 'cust*' pattern from the 'data' directory into the 'multi/raw' blob/folder within the 'test' container. 
```python
client.upload_file(
    container_name='test',
    blob_name='multi/raw',
    all_files=True,
    file_path='data',
    file_regex='cust*'
    )
```

### Uploads all files from a specified folder to a blob/folder.
This code utilizes the upload_file() method from the AzureStorageUtils with the all_files=True parameter to upload all files from the 'Data_Profiling/data' directory into the 'raw' blob/folder within the 'test' container. 
```python
client.upload_file(
    container_name='test',
    blob_name='raw',
    all_files=True,
    file_path='Data_Profiling/data'
    )
```

### Deletes all files inside a blob.
This code uses the delete_file() method from the AzureStorageUtils instance (client) with the all_files=True parameter to delete all files contained within the 'raw' blob inside the 'test' container.
```python
client.delete_file(
    container_name='test',
    blob_name='raw',
    all_files=True
    )
```

### Deletes files with with specific naming pattern inside a blob.
This code utilizes the delete_file() method from the AzureStorageUtils with the all_files=True parameter and the file_regex='cust*' parameter to delete all files inside the 'raw' blob that match the specified pattern ('cust*').
```python
client.delete_file(
    container_name='test',
    blob_name='raw',
    all_files=True,
    file_regex='cust*'
    )
```

### Deletes a single file inside a blob.
This code uses the delete_file() method from the AzureStorageUtils instance (client) to delete the 'Product_data.csv' file from the 'raw' blob in the 'test' container. 
```python
client.delete_file(
    container_name='test',
    blob_name='raw',
    file_name='Product_data.csv'
    )
```

### Deletes a container.
This code uses the delete_container() method from the AzureStorageUtils instance (client) to delete the container named 'test' from the Azure storage account.
```python
client.delete_container(container_name='test')
```

### Deletes all containers from the storage account
This code utilizes the delete_container() method from the AzureStorageUtils instance (client) with the all_containers=True parameter to delete all containers present in the Azure storage account. Use this cautiously as it will delete all containers and their contents.
```python
client.delete_container(all_containers=True)
```

## Contributing
our contributions to this project are encouraged and valued! You have the autonomy to make modifications directly and merge them without the need for a formal review process.
You have the freedom to utilize the code in any manner you deem fit. However, please be cautious as any deletion or modification of critical containers or data is not my responsibility.
To contribute:

    Fork the repository to your GitHub account.
    Clone the forked repository to your local machine.
    Create a new branch for your changes: git checkout -b feature/YourFeature
    Implement your modifications or additions.
    Commit your changes: git commit -am 'Add new feature'
    Push the branch to your GitHub repository: git push origin feature/YourFeature
    Merge your changes directly into the main branch.

Your contributions directly impact the project. Please ensure that your modifications align with the project's goals and standards. Thank you for your valuable contributions!
