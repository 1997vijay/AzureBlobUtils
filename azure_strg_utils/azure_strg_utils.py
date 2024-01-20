"""
Azure Storage Utils facilitates interaction with Azure Storage services, offering a versatile set of functionalities. 
It enables users to perform various operations on storage accounts, including uploading, downloading, deleting, and copying files with ease.

Dependencies:
    - pandas: Data manipulation and analysis library.
    - azure-storage-blob: Azure python sdk to intrect with azure storage services.
    - fnmatch: This module is used for matching Unix shell-style wildcards.

Usage:
    1. Ensure that you have the required dependencies installed.
    2. Set up your azure storage connection string or provide them through other means (e.g., environment variables).
    3. Run the code to intrect with azure storage.
    4. Below are the activity that we can perform on storage account
        1. Create container
        2. List Containers
        3. List Blobs
        4. Download single file or specific file or entire blob
        5. Upload single or specific file
        6. Delete single or specific file from blob
        7. Delete single or all containers
        8. Copy from one container to another container in the same storage account.
        9. Download or Delete based on the date and condition like greater_than, less_than etc.
        10. We can apply file regex to download ,delete or upload files that have specific pattern.
        11. Upload Pandas dataframe to blob.

Author Information:
    \n - Name: Vijay Kumar
    \n - Email: vijay.kumar.1997@outlook.com
    \n - Github: https://github.com/1997vijay/AzureStorageUtils

"""

from azure.storage.blob import BlobServiceClient,BlobBlock
from datetime import datetime
from io import BytesIO

import os
import pandas as pd
import uuid

from . import utils
from .exceptions import *

#set the current timestamp
currentTime=datetime.now()

class AzureStorageUtils:
    def __init__(self,connection_string) -> None:
        """
        Initialize the object with Azure Storage connection string.

        \n Args:
            - connection_string (str): Azure storage account connection string.

        \n Example:
            >>> connection_str = "your_azure_storage_connection_string"
            >>> client = AzureStorageUtils(connection_str)
        """
        self.__connection_string=connection_string

        if self.__connection_string!='' or self.__connection_string is not None:
            try:
                self._client=BlobServiceClient.from_connection_string(conn_str=self.__connection_string)
            except Exception as e:
                raise e
        else:
            raise ValueError('Invalid connection string!!')

    def list_container(self):
        """
        \n Get the list of containers present in the storage account.

        \n Returns:
            - List of containers.

        \n Example:
            >>> container = client.list_containers()
        """
        containers_list=[]
        try:
            containers = self._client.list_containers()
            containers_list=[container.name for container in containers]
        except Exception as e:
            raise e
        return containers_list
    
    def list_blobs(self,container_name):
        """
        Get the list of blobs/folders present in a container.

        \n Args:
            - container_name (str): Container name.

        \n Returns:
            - List of blobs/folders.

        \n Example:
            >>> container = client.list_blobs(container_name='test')
        """

        folder_list=[]
        try:
            container_client=self._client.get_container_client(container_name)
            blob=container_client.list_blobs()
            folder_list=[file.name.split('/')[0] for file in blob]
        except Exception as e:
            raise e
        return list(set(folder_list))
    
    def list_files(self, container_name, blob_name):
        """
        Get the list of blobs/folders present in a container.

        \n Args:
            - container_name (str): Container name.
            - blob_name (str): Blob name.

        \n Returns:
            - List of blobs/folders.

        \n Example:
            >>> # Get the list of files in a container
            >>> container_name = "your_container_name"
            >>> blob_name = "your_blob_name"
            >>> files = storage_utils.list_files(container_name, blob_name)
            >>> print(files)
            ['file1.txt', 'file2.csv', 'folder1', 'folder2']
        """
        files=[]
        try:
            container_client=self._client.get_container_client(container=container_name)
            blob=container_client.list_blobs(name_starts_with=blob_name)
            for file in blob:
                # name_starts_with will retrun all the matching blobs, to filter out specific blob we can split by /
                if file.name.split("/")[0]==blob_name:
                    file_name=file.name.replace(f"{blob_name}/",'')
                    if file_name!='' and file_name!=blob_name:
                        files.append(file_name)
        except Exception as e:
            raise e
        return files
    
    def download_file(self,container_name,
                      blob_name,
                      file_name:str=None,
                      path:str='download',
                      is_dataframe:bool=False,
                      all_files:bool=False,
                      file_regex:str=None):
        """
        \n Download a file/all files from Azure Blob Storage.
        \n Note: If there are subdirectory in blob, it is going to download subdirectory files as well.
        \n Args:
            - container_name (str): Container name.
            - blob_name (str): Blob name.
            - file_name (str): File name to be downloaded.

        \n Kwargs:
            - path (str, optional): Location where the file will be saved. Default is '/download'.
            - is_dataframe (bool, optional): Return a dataframe without downloading the file. Default is False.
            - all_files (bool, optional): download all file present in a blob
            - file_regex (str, optional): You can pass the regex expresion to filter the files

        \n Example:
            >>> # download sales_data.csv file from test blob into test folder
            >>> client.download_file(
                container_name='rawdata',
                blob_name='test',
                file_name='sales_data.csv',
                path='test')

            >>> # download all files present in 'raw' blob
            >>> client.download_file(
                container_name='rawdata',
                blob_name='raw',
                path='downloaded',
                all_files=True
                )

            >>> # download all files from 'raw' folder whose name start with 'cust'
            >>> client.download_file(
                container_name='rawdata',
                blob_name='raw',
                all_files=True,
                path='downloaded',
                file_regex='cust*'
                )

            >>> # return a pandas dataframe
            >>> df=client.download_file(
                        container_name='rawdata',
                        blob_name='raw',
                        file_name='cars_new.csv',
                        is_dataframe=True
                        )
        """

        try:
            if not is_dataframe:
                full_path = os.path.join(os.getcwd(),f"{path}/{blob_name}")
                current_path=full_path
                if not os.path.exists(full_path):
                    os.makedirs(full_path)
            is_data=False
            if is_dataframe:
                blob_client=self._client.get_blob_client(container=container_name,blob=f"{blob_name}/{file_name}")
                stream = BytesIO()
                blob_client.download_blob().readinto(stream)
                stream.seek(0)
                df=utils._read_file(file_name,stream)
                return df
            
            elif all_files:
                file_list=self.list_files(container_name=container_name,blob_name=blob_name)
                file_list=utils._filter_file(file_regex=file_regex,file_list=file_list)

                count=0
                for file in file_list:
                    current_path=full_path
                    is_data=False
                # Creating directories and moving files
                    if '/' in file:
                        if '.' not in file:
                            current_path = os.path.join(current_path, file)
                            if not os.path.exists(current_path):
                                os.makedirs(current_path)
                        else:
                            current_path = os.path.join(current_path, file)

                            blob_client=self._client.get_blob_client(container=container_name,blob=f"{blob_name}/{file}")
                            try:
                                data=blob_client.download_blob().readall()
                                is_data=True
                            except Exception as e:
                                raise e
                            if is_data:
                                with open(current_path,"wb") as f:
                                    f.write(data)
                                    count=count+1

                    elif '.' not in file:
                        current_path = os.path.join(current_path, file)
                        if not os.path.exists(current_path):
                                os.makedirs(current_path)
                    else:

                        blob_client=self._client.get_blob_client(container=container_name,blob=f"{blob_name}/{file}")
                        try:
                            data=blob_client.download_blob().readall()
                            is_data=True
                        except Exception as e:
                            raise e

                        if is_data:
                            with open(f"{full_path}/{file}","wb") as f:
                                f.write(data)
                                count=count+1
                print(f'{count} files downloaded from container: {container_name} successfully')
            else:
                if file_name and not isinstance(file_name, int) and file_name.strip():
                    try:
                        blob_client=self._client.get_blob_client(container=container_name,blob=f"{blob_name}/{file_name}")
                        data=blob_client.download_blob().readall()
                        is_data=True
                    except Exception as e:
                        raise e

                    if is_data:
                        with open(f"{full_path}/{file_name}","wb") as f:
                            f.write(data)
                        print(f'{file_name} downloaded from container: {container_name} successfully')
                else:
                    raise ValueError('Invalid file name!!')
        except Exception as e:
            raise e

    def _upload_file_chunks(self,blob_client,full_path,file_name):
        """
        Upload a large file to a blob in chunks.

        \n Args:
            - blob_client: The blob client for the destination blob.
            - full_path (str): The full local path to the file to be uploaded.
            - file_name (str): The name of the file to be uploaded.
        \n Returns:
            - dict:
                A dictionary containing information about the uploaded file.

        \n Raises:
            - UploadFileError:
                Raised if an error occurs during the file upload process.
        \n Note:
            This function is designed for uploading large files in smaller chunks to Azure Blob Storage.
        """
        try:
            # upload data
            block_list=[]
            chunk_size=1024*1024*4 #4 MB chunk
            with open(f"{full_path}/{file_name}","rb") as f:
                while True:
                    read_data = f.read(chunk_size)
                    if not read_data:
                        break # done
                    blk_id = str(uuid.uuid4())
                    blob_client.stage_block(block_id=blk_id,data=read_data) 
                    block_list.append(BlobBlock(block_id=blk_id))
            result=blob_client.commit_block_list(block_list)
            return result
        except BaseException as err:
            raise UploadFileError(f'Error while uploading the file.,{err}')

    def upload_file(self,container_name,blob_name,file_path,file_name:str=None,all_files:bool=False,file_regex:str=None):
        """
        Upload a file from a local directory to Azure Blob Storage.
        \n Args:
            - container_name (str): Container name.
            - blob_name (str): Blob name.
            - file_path (str): Local file path.

        \n Kwargs:
            - file_name (str, optional): File name. Default is None.
            - all_files (bool, optional): If True, upload all files from the given directory. Default is False.
            - file_regex (str, optional): You can pass the regex expresion to filter the files

        \n Example:
            >>> # upload single file 'cars_new.csv' from specified path file_path 'data' into blob/folder 'raw' 
            >>> file_status=client.upload_file(
                container_name='rawdata',
                blob_name='test',
                file_name='cars_new.csv',
                file_path='data'
                )
                print(file_status)

            >>> # upload all file present inside data folder
            >>> file_status=client.upload_file(
                                    container_name='rawdata',
                                    blob_name='test_new',
                                    all_files=True,
                                    file_path='data'
                                    )
                print(file_status)

            >>> # upload all files whose name start with 'cust*' from specified folder to specified blob
            >>> file_status=client.upload_file(
                container_name='rawdata',
                blob_name='raw',
                all_files=True,
                file_path='data',
                file_regex='cust*'
                )
                print(file_status)

        """
        full_path = os.path.join(os.getcwd(),file_path)
        try:
            file_status=[]
            if all_files:
                files=os.listdir(full_path)
                files=utils._filter_file(file_regex=file_regex,file_list=files)
                print(f'Uploading {len(files)} files !!')

                flag=False
                count=0
                if len(files)==0:
                    raise EmptyFolderError('Folder does not contain any files.')
                else:
                    for file in files:
                        count=count+1
                        print(f'{count}/{len(files)} done.',end='\r')

                        # get the file size
                        file_stats = os.stat(file_path+'/'+file)
                        file_size=round(file_stats.st_size / (1024 * 1024),2)

                        # get blob client and upload file
                        blob_client=self._client.get_blob_client(container=container_name,blob=f"{blob_name}/{file}")
                        result=self._upload_file_chunks(blob_client,full_path,file)

                        # append extra info in result dict
                        result['File Name']=file
                        result['Upload Timestamp']=currentTime
                        result['File Size(MB)']=file_size
                        file_status.append(result)

                print(f'{len(files)} files uploaded to container: {container_name} successfully')
            else:
                # get the file size
                file_stats = os.stat(file_path+'/'+file_name)
                file_size=round(file_stats.st_size / (1024 * 1024),2)

                # get client and uplaod file
                blob_client=self._client.get_blob_client(container=container_name,blob=f"{blob_name}/{file_name}")
                result=self._upload_file_chunks(blob_client,full_path,file_name)

                result['File Name']=file_name
                result['Upload Timestamp']=currentTime
                result['File Size(MB)']=file_size
                file_status.append(result)
                print(f'{file_name} file uploaded to container: {container_name} successfully')

            return file_status
        except Exception as e:
            message=f'Error while uploading the file. {e}'
            raise UploadFileError(message)
            
    
    def delete_file(self,container_name,blob_name,file_name:str=None,all_files:bool=False,file_regex:str=None):
        """
        Delete files from Azure Blob Storage.
        \n Args:
            - container_name (str): Container name.
            - blob_name (str): Blob name from which the file will be deleted.
            - file_name (str): File name to be deleted.

        \n Kwargs:
            - all_files (bool, optional): If True, delete all files from the given blob. Default is False.
            - file_regex (str, optional): You can pass the regex expresion to filter the files

        \n Example:
            >>> # delete all files present inside a blob
            >>> client.delete_file(
                container_name='rawdata',
                blob_name='raw',
                all_files=True
                )

            >>> # delete single files present inside a blob
            >>> client.delete_file(
                container_name='rawdata',
                blob_name='raw',
                file_name='Product_data.csv'
                )

            >>> # delete all files present inside a blob whose name start with 'cust'
            >>> client.delete_file(
                container_name='rawdata',
                blob_name='raw',
                all_files=True,
                file_regex='cust*'
                )
        """

        try:
            result=False
            if all_files:
                file_list=self.list_files(container_name=container_name,blob_name=blob_name)
                file_list=utils._filter_file(file_regex=file_regex,file_list=file_list)
                for file in file_list:
                    blob_client=self._client.get_blob_client(container=container_name,blob=f"{blob_name}/{file}")
                    blob_client.delete_blob(delete_snapshots='include')

                print(f'{len(file_list)} files deleted from container: {container_name} successfully')
                result=True
            else:
                blob_client=self._client.get_blob_client(container=container_name,blob=f"{blob_name}/{file_name}")
                blob_client.delete_blob(delete_snapshots='include')
                print(f'{file_name} deleted from container: {container_name} successfully')
                result=True
        except Exception as e:
            message=f'Error while deleting the file!!,{e}'
            raise DeleteFileError(message)
        return result
    
    def conditional_filter(self, container_name, blob_name, creation_date, comparison='less_than', file_regex=None):
        """
        Filter files based on certain criteria for deletion from Azure Blob Storage.
        It will return the list od file on which we can take any action like delete file,copy file,download file etc.

        \n Args:
            - container_name (str): Name of the container in Azure Blob Storage.
            - blob_name (str): Blob name from which the files will be filtered.
            - creation_date (str): Date for comparison in the format '%Y-%m-%d'.
            - comparison (str, optional): The type of comparison for file filtering (default is 'less_than').
            - file_regex (str, optional): The regex expression used to filter the files (default is None).

        \n Returns:
            - list: A list of files filtered based on the specified criteria.

        \n Explanation:
            This method filters files in the Azure Blob Storage based on the specified criteria.
            It retrieves a list of files from the blob specified by 'blob_name' in the 'container_name'.
            Then, it compares the creation date of each file with the 'creation_date' using the specified comparison
            operator (default is 'less_than'), and collects the files that meet the criteria for deletion/download.
            If 'file_regex' is provided, it further filters the files using the regex pattern.
            Returns a list of files that satisfy the criteria.

        \n Example:
            >>> # get all files which have creation date greater than '2023-12-15'
            >>> file_list=client.conditional_filter(
                container_name='rawdata',
                blob_name='raw',
                creation_date='2023-12-15',
                comparison='greater_than'
                )
                print(file_list)

        """
        try:
            if comparison not in ['less_than','less_than_or_equal','greater_than','greater_than_or_equal']:
                raise ValueError("Invalid comparison operator. Use any one of them ['less_than','less_than_or_equal','greater_than','greater_than_or_equal']")
            
            file_list = []
            creation_date = datetime.strptime(creation_date, '%Y-%m-%d').date()

            container_client = self._client.get_container_client(container=container_name)
            blob = container_client.list_blobs(name_starts_with=blob_name)
            comparison_operator = utils._comparison_operator(comparison=comparison)

            # Collect files based on date criteria
            for file in blob:
                if comparison_operator(file.creation_time.date(), creation_date):
                    file_name = file.name.replace(f"{blob_name}/", '')
                    if file_name != '' and file_name != blob_name:
                        file_list.append(file_name)

            # Filter files by regex pattern if provided
            file_list = utils._filter_file(file_regex=file_regex, file_list=file_list)
            return file_list
        except Exception as e:
            raise e
        
    def conditional_operation(self, 
                              container_name, 
                              blob_name, 
                              creation_date, 
                              comparison='less_than', 
                              file_regex=None,
                              action='download',
                              path='download'):
            """
            Perform conditional operations on files in Azure Blob Storage based on specified criteria.
            \n Args:
                - container_name (str): The name of the container in Azure Blob Storage.
                - blob_name (str): The specific blob name in the container.
                - creation_date (str): The creation date as a string in 'YYYY-MM-DD' format.
                - comparison (str, optional): The type of comparison operator for date-based deletion.
                                            Possible values: 'less_than', 'less_than_or_equal',
                                            'greater_than', 'greater_than_or_equal', or any other value
                                            (defaults to 'less_than').
                - file_regex (str, optional): Regex pattern to filter files for deletion. Defaults to None.
                - action (str, optional): The action to perform - 'delete' or 'download'. Defaults to 'download'.
                - path (str, optional): The path to download files when action is set to 'download'. Defaults to 'download'.

            \n Raises:
                - Exception: If an error occurs during file deletion or download, it raises an exception.

            \n Returns:
                - None: Performs file deletion or downloads files based on the specified action.

        \n Example:
            >>> # download all files which have creation date greater than '2023-12-15'
            >>> client.conditional_operation(
                container_name='rawdata',
                blob_name='raw',
                creation_date='2023-12-15',
                comparison='greater_than',
                action='download',
                path='data'
                )

            >>> # delete all files which have creation date less than '2023-12-15'
            >>> client.conditional_operation(
                container_name='rawdata',
                blob_name='raw',
                creation_date='2023-12-15',
                comparison='less_than',
                action='delete'
                )

            >>> # download all files which have creation date greater than '2023-12-15' and whose name start with 'cust'
            >>> client.conditional_operation(
                container_name='rawdata',
                blob_name='raw',
                creation_date='2023-12-15',
                comparison='greater_than',
                action='download',
                path='data',
                file_regex='cust*'
                )

            >>> # delete all files which have creation date less than '2023-12-15' and whose name start with 'cust'
            >>> client.conditional_operation(
                container_name='rawdata',
                blob_name='raw',
                creation_date='2023-12-15',
                comparison='less_than',
                action='delete',
                file_regex='cust*'
                )
            """
            try:
                file_list=self.conditional_filter(container_name, blob_name, creation_date, comparison, file_regex)
                
                if action=='delete':
                    for file in file_list:
                        self.delete_file(container_name=container_name,blob_name=blob_name,file_name=file)
                    print(f'{len(file_list)} files deleted from container: {container_name} successfully')

                elif action=='download':
                    for file in file_list:
                        self.download_file(container_name=container_name,blob_name=blob_name,file_name=file,path=path)
                    print(f'{len(file_list)} files downloaded from container: {container_name} successfully')
                else:
                    raise ValueError("Not a valid action type. Supported action types are ['download','delete'].")
            except Exception as e:
                message = f'Error while {action} the file!!,{e}'
                raise ConditionalOperationError(message)
        
    def copy_blob(self,container_name,
                  blob_name,
                  destination_container,
                  destination_blob,
                  creation_date:str=None,
                  comparison:str='less_than',
                  file_name:str=None,
                  all_files:bool=False,
                  file_regex:str=None,
                  abort_after:int=100,
                  delete_file:bool=False
                  ):
        """
        Copy files or specific file from one Azure Blob Storage container to another.
        
        \n Args:
            - container_name (str): Source container name.
            - blob_name (str): Source blob name or pattern to filter files.
            - destination_container (str): Destination container name.
            - destination_blob (str): Destination blob name.
        \n Kwargs:
            - file_name (str, optional): Specific file name to copy. Default is None.
            - all_files (bool, optional): If True, copy all files from the source blob. Default is False.
            - file_regex (str, optional): Regex pattern to filter files for copying.
            - abort_after (int, optional): Abort the copy after given time (in seconds). Default is 100 seconds.
            - delete_file (boolean,optional): Delete the file after copy. Default False.
        \n Raises:
            - Exception: Raises an exception if an error occurs during the copying process.

        \n Example:
            >>> # copy the sales_data.csv file from container 'rawdata' to 'destination_container'
            >>> status=client.copy_blob(
                container_name='rawdata',
                blob_name='raw',
                destination_container='new-test',
                destination_blob='raw',
                file_name='sales_data.csv'
                )
                print(status)

            >>> # copy the sales_data.csv file from container 'rawdata' to destination container 'new-test'
            >>> # and delete the file after copy from source container 'rawdata'
            >>> status=client.copy_blob(
                container_name='rawdata',
                blob_name='raw',
                destination_container='new-test',
                destination_blob='raw',
                file_name='sales_data.csv',
                delete_file=True
                )
                print(status)

            >>> # copy the all files from container 'rawdata' to destination container 'new-test'
            >>> status=client.copy_blob(
                container_name='rawdata',
                blob_name='raw',
                destination_container='new-test',
                destination_blob='raw',
                all_files=True
                )
                print(status)

            >>> # copy the all files from container 'rawdata' to destination container 'new-test' and whose name start with 'cust'.
            >>> status=client.copy_blob(
                container_name='rawdata',
                blob_name='raw',
                destination_container='new-test',
                destination_blob='raw',
                all_files=True,
                file_regex='cust*
                )
                print(status)
        \n Note: Priority will be all_files > creation_date > file_name
        \n Abort time by default is 100s which can be changes using 'abort_after' parameter.

        """
        try:
            file_status=[]
            if all_files:
                file_list=self.list_files(container_name,blob_name)
                file_list=utils._filter_file(file_regex,file_list)

                for file in file_list:
                    # source and destination client
                    source_blob_client=self._client.get_blob_client(container=container_name,blob=f'{blob_name}/{file}')
                    destination_blob_client=self._client.get_blob_client(container=destination_container,blob=f'{destination_blob}/{file}')

                    # copy file
                    result=destination_blob_client.start_copy_from_url(source_url=source_blob_client.url)
                    if delete_file:
                        self.delete_file(container_name=container_name,blob_name=blob_name,file_name=file_name)
                    
                    # append extra info in result dict
                    result['File Name']=file
                    result['Timestamp']=currentTime
                    file_status.append(result)
                print(f'{len(file_list)} files copied successfully')
            elif creation_date!=None:
                file_list=self.conditional_filter(container_name, blob_name, creation_date, comparison, file_regex)
                for file in file_list:
                    # source and destination client
                    source_blob_client=self._client.get_blob_client(container=container_name,blob=f'{blob_name}/{file}')
                    destination_blob_client=self._client.get_blob_client(container=destination_container,blob=f'{destination_blob}/{file}')

                    # copy file
                    result=destination_blob_client.start_copy_from_url(source_url=source_blob_client.url)
                    if delete_file:
                        self.delete_file(container_name=container_name,blob_name=blob_name,file_name=file_name)

                    # append extra info in result dict
                    result['File Name']=file
                    result['Timestamp']=currentTime
                    file_status.append(result)
                print(f'{len(file_list)} files copied successfully')
            else:
                if file_name!=None:
                    # source and destination client
                    source_blob_client=self._client.get_blob_client(container=container_name,blob=f'{blob_name}/{file_name}')
                    destination_blob_client=self._client.get_blob_client(container=destination_container,blob=f'{destination_blob}/{file_name}')

                    # copy and delete the file
                    result=destination_blob_client.start_copy_from_url(source_url=source_blob_client.url)
                    if abort_after:
                        abort_result=utils._abort_copy(blob_client=destination_blob_client,abort_time=abort_after)
                        if abort_result:
                            if delete_file:
                                self.delete_file(container_name=container_name,blob_name=blob_name,file_name=file_name)
                            print(f'{file_name} file copied from container: {container_name}/{blob_name} to {destination_container}/{destination_blob}')
                        else:
                            print('Copy operation aborted!!.')
                            return file_status
                        
                    # append extra info in result dict
                    result['File Name']=file_name
                    result['Timestamp']=currentTime
                    file_status.append(result)
                else:
                    raise ValueError('Invalid file name.')
            return file_status
        except Exception as e:
            raise e
        
    def upload_dataframe(self,dataframe,file_name,container_name,blob_name):
        """
        Uploads a Pandas DataFrame to Azure Blob Storage.

        \n Args:
            - dataframe (pd.DataFrame): The Pandas DataFrame to be uploaded.
            - file_name (str): The name of the file to be created in the blob storage.
            - container_name (str): The name of the Azure Blob Storage container.
            - blob_name (str): The name of the blob within the specified container.
        \n Raises:
            - Exception: If an error occurs during the uploading process.
        \n Example:
            >>> # Upload the Pandas dataframe to blob processed in XML format.
            >>> # supported format are CSV,JSON,XML.
            >>> result=client.upload_dataframe(
                                dataframe=df,
                                file_name='cars.xml',
                                container_name='rawdata',
                                blob_name='processed'
                                )
                print(result)
        """
        try:
            format=file_name.split(".")[-1]
            if isinstance(dataframe,pd.DataFrame):
                print('Uploading the dataframe.')
                file_content = BytesIO()
                utils._save_dataframe(file_content,dataframe,format)
                file_content.seek(0)
                blob_client=self._client.get_blob_client(container=container_name,blob=f"{blob_name}/{file_name}")
                result=blob_client.upload_blob(data=file_content,overwrite=True)
                return result
        except Exception as e:
            raise e    

    def create_container(self,container_name):
        """
        Create a container in Azure Storage.
        \n Args:
            - container_name (str): Name of the new container.
        \n Example:
            >>> # create new container test2
            >>> client.create_container(container_name='test2')
        """
        try:
            # create containers
            self._client.create_container(container_name)
            # list containers
            container_list=self.list_container()
            print(f'Available containers: {container_list}')
        except Exception as e:
            raise e
    
    def delete_container(self,container_name:str=None,all_containers:bool=False):
        """
        Delete a container from Azure Storage.

        \n Args:
            - container_name (str): Name of the container to be deleted.
        \n Kwargs:
            - all_container (bool, optional): If True, delete all containers. Default is False.
        \n Example:
            >>> # delete container test2
            >>> client.delete_container(container_name='test2')

            >>> # delete all container
            >>> client.delete_container(all_containers=True)
        \n Note: We carefull while using this method as it will delete the container and its file.
        """

        try:
            if all_containers:
                containers=self.list_container()
                print(f'Deleting {len(containers)} containers')

                for container in containers:
                    container_client=self._client.get_container_client(container=container)
                    container_client.delete_container()
                    print(f'Container {container_name} deleted successfully!!')
            else:
                # delete container
                container_client=self._client.get_container_client(container=container_name)
                container_client.delete_container()
                print(f'Container {container_name} deleted successfully!!')

                # list containers
                container_list=self.list_container()
                print(f'Available containers: {container_list}')
        except Exception as e:
            raise e
