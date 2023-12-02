from azure.storage.blob import BlobServiceClient,BlobBlock
from azure.core.exceptions import ServiceRequestError
import os
import sys
import pandas as pd
import fnmatch
from io import BytesIO
import uuid



class AzureFileHandler:
    def __init__(self,connection_string) -> None:
        """Initialize the object with Azure Storage connection string.

        Args:
            connection_string (str): Azure storage account connection string.
        """

        self.__connection_string=connection_string

        if self.__connection_string!='' or self.__connection_string is not None:
            try:
                self._client=BlobServiceClient.from_connection_string(conn_str=self.__connection_string)
            except ServiceRequestError as e:
                raise ServiceRequestError("Error while connecting to blob!!. {e}")
        else:
            raise ValueError('Invalid connection string!!')
    
    def list_container(self):
        """Get the list of containers present in the storage account.

        Returns:
            List of containers.
        """

        containers_list=[]
        try:
            containers = self._client.list_containers()
            for container in containers:
                containers_list.append(container.name)
            return containers_list
        except Exception as e:
            message=f"Error while getting container list!!, {e}"
            raise message
    
    def list_folders(self,container_name):
        """Get the list of blobs/folders present in a container.

        Returns:
            List of blobs/folders.
        """

        folder_list=[]
        try:
            container_client=self._client.get_container_client(container_name)
            blob=container_client.list_blobs()
            for file in blob:
                folder_list.append(file.name.split('/')[0])
            return list(set(folder_list))
        except Exception as e:
            raise (f"Container not found !!,{e}")
    
    def list_files(self,container_name,blob_name):
        """Get the list of blobs/folders present in a container.

        Returns:
            List of blobs/folders.
        """

        try:
            container_client=self._client.get_container_client(container=container_name)
            blob=container_client.list_blobs(name_starts_with=blob_name)

            files=[]
            for file in blob:
                file_name=file.name.replace(f"{blob_name}/",'')
                if file_name!='' and file_name!=blob_name:
                    files.append(file_name)
            return blob, files
        except Exception as e:
            message=f"Container not found !!,{e}"
            raise message
    
    def download_file(self,container_name,blob_name,file_name:str=None,path:str='download',is_dataframe:bool=False,all_files:bool=False,file_regex:str=None):
        """Download a file/all files from Azure Blob Storage.

        Args:
            container_name (str): Container name.
            blob_name (str): Blob name.
            file_name (str): File name to be downloaded.

        Kwargs:
            path (str, optional): Location where the file will be saved. Default is './download'.
            is_dataframe (bool, optional): Return a dataframe without downloading the file. Default is False.
            all_files (bool, optional): download all file present in a blob
        """

        try:
            full_path = os.path.join(os.getcwd(),f"{path}/{blob_name}")
            current_path=full_path
            if not os.path.exists(full_path):
                os.makedirs(full_path)

            is_data=False
            if is_dataframe:
                blob_client=self._client.get_blob_client(container=container_name,blob=f"{blob_name}/{file_name}")
                stream = BytesIO()
                blob_client.download_blob().download_to_stream(stream)
                stream.seek(0)

                name, extension = file_name.split('.')
                extension = extension.lower()

                if extension == 'csv':
                    df = pd.read_csv(stream)
                elif extension == 'xlsx':
                    df = pd.read_excel(stream)
                elif extension == 'json':
                    df = pd.read_json(stream)
                else:
                    raise ValueError(f"Unsupported file format: {extension}")
                return df
            
            elif all_files:
                blob,file_list=self.list_files(container_name=container_name,blob_name=blob_name)
                file_list=self._filter_file(file_regex=file_regex,file_list=file_list)

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

            else:
                if file_name and not isinstance(file_name, int) and file_name.strip():
                    try:
                        blob_client=self._client.get_blob_client(container=container_name,blob=f"{blob_name}/{file_name}")
                        data=blob_client.download_blob().readall()
                        is_data=True
                    except Exception as e:
                        sys.exit(e)

                    if is_data:
                        with open(f"{full_path}/{file_name}","wb") as f:
                            f.write(data)
                            print(f'{file_name} downloaded from container: {container_name} successfully')
                else:
                    sys.exit('Invalid file name!!')
            print(f'{count} files downloaded from container: {container_name} successfully')
        except Exception as e:
            message=f'Error while downloading the blob!!,{e}'
            raise message
    
    def upload_file(self,container_name,blob_name,file_path,file_name:str=None,all_files:bool=False,file_regex:str=None):
        """Upload a file from a local directory to Azure Blob Storage.

        Args:
            container_name (str): Container name.
            blob_name (str): Blob name.
            file_path (str): Local file path.

        Kwargs:
            file_name (str, optional): File name. Default is None.
            all_files (bool, optional): If True, upload all files from the given directory. Default is False.
        """

        try:
            if all_files:
                files=os.listdir(file_path)
                files=self._filter_file(file_regex=file_regex,file_list=files)
                print(f'Uploading {len(files)} files !!')

                flag=False
                count=0
                if len(files)==0:
                    sys.exit('Empty folder!!')
                else:
                    for file in files:
                        count=count+1
                        print(f'{count}/{len(files)} done.',end='\r')
                        file_stats = os.stat(file_path+'/'+file)
                        file_size=round(file_stats.st_size / (1024 * 1024),2)

                        blob_client=self._client.get_blob_client(container=container_name,blob=f"{blob_name}/{file}")

                        # send large file in chunk of 100 MB
                        if file_size>200:
                                block_list=[]
                                chunk_size=1024*1024*100

                                with open(f"{file_path}/{file}","rb") as f:

                                    while True:
                                        read_data = f.read(chunk_size)
                                        if not read_data:
                                            break
                                        blk_id = str(uuid.uuid4())
                                        blob_client.stage_block(block_id=blk_id,data=read_data)
                                        block_list.append(BlobBlock(block_id=blk_id))
                                blob_client.commit_block_list(block_list)
                        else:
                            with open(f"{file_path}/{file}","rb") as f:
                                data=f.read()
                                result=blob_client.upload_blob(data,overwrite=True)
                                if result['request_id']:
                                    flag=True
                if flag:
                    print(f'{len(files)} files uploaded to container: {container_name} successfully')
                else:
                    print('Something went wrong!!')
            else:
                blob_client=self._client.get_blob_client(container=container_name,blob=f"{blob_name}/{file_name}")
                with open(f"{file_path}/{file_name}","rb") as f:
                    data=f.read()
                    result=blob_client.upload_blob(data,overwrite=True)
                    if result['request_id']:
                        print(f'{file_name} uploaded to container: {container_name} successfully')

        except Exception as e:
            message=f'Error while uploading the file!!,{e}'
            raise message
    
    def delete_file(self,container_name,blob_name,file_name:str=None,all_files:bool=False,file_regex:str=None):
        """Delete files from Azure Blob Storage.

        Args:
            container_name (str): Container name.
            blob_name (str): Blob name from which the file will be deleted.
            file_name (str): File name to be deleted.

        Kwargs:
            all_files (bool, optional): If True, delete all files from the given blob. Default is False.
        """

        try:
            if all_files:
                blob,file_list=self.list_files(container_name=container_name,blob_name=blob_name)
                file_list=self._filter_file(file_regex=file_regex,file_list=file_list)
                sub_folders=[item for item in file_list if '/' in item]
                if len(sub_folders)!=0:
                    pass
                for file in file_list:
                    blob_client=self._client.get_blob_client(container=container_name,blob=f"{blob_name}/{file}")
                    blob_client.delete_blob(delete_snapshots='include')

                print(f'{len(file_list)} files deleted from container: {container_name} successfully')
            else:
                blob_client=self._client.get_blob_client(container=container_name,blob=f"{blob_name}/{file_name}")
                blob_client.delete_blob(delete_snapshots='include')
                print(f'{file_name} deleted from container: {container_name} successfully')
        except Exception as e:
            message=f'Error while deleting the file!!,{e}'
            raise message
        
    def _filter_file(self,file_regex,file_list):
        if file_regex!=None and not isinstance(file_regex, int):
            file_list = fnmatch.filter(file_list, file_regex)
        else:
            file_list=file_list
        return file_list

    def create_container(self,container_name):
        """Create a container in Azure Storage.

        Args:
            container_name (str): Name of the new container.
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
        """Delete a container from Azure Storage.

        Args:
            container_name (str): Name of the container to be deleted.

        Kwargs:
            all_container (bool, optional): If True, delete all containers. Default is False.
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
    
