from azure_strg_utils import AzureStorageUtils
import pytest
import pandas as pd
import os

@pytest.fixture
def client():
    CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=adfstorage1140;AccountKey=546RzkYi4oCkyT6TtZzbtieke5ksF12cMMCYlMcufpducNYja69BI9z19zDzDaVG+xbA6InpCbjE+AStXbdNqA==;EndpointSuffix=core.windows.net"
    client=AzureStorageUtils(connection_string=CONNECTION_STRING)
    return client

def test_list_container(client):
    container=client.list_container()
    assert type(container)==list,'Not a list'

def test_list_blobs(client):
    blobs=client.list_blobs(container_name='rawdata')
    assert type(blobs)==list, 'Not a list'

def test_list_files(client):
    files=client.list_files(container_name='rawdata',blob_name='raw')
    assert type(files)==list, 'Not a list'

def test_download_file_dataframe(client):
    df=client.download_file(container_name='rawdata',blob_name='raw',file_name='cars_new.csv',is_dataframe=True)
    assert isinstance(df,pd.DataFrame), 'Not a pandas Dataframe'

def test_download_all_files(client,capsys):
    # Test downloading all files from a blob
    container_name = "rawdata"
    blob_name = "raw"
    path = 'test'
    client.download_file(container_name=container_name,blob_name=blob_name,path=path,all_files=True)

    # Check if the files are downloaded
    assert os.path.exists(os.path.join(path, blob_name, "customer_data.csv")), 'File does not exits'
    assert os.path.exists(os.path.join(path, blob_name, "cars_new.csv")),'File does not exits'

def test_download_single_file(client):
    # Test downloading all files from a blob
    container_name = "rawdata"
    blob_name = "raw"
    path = 'test'
    file_name='cars_new.csv'
    client.download_file(container_name=container_name,blob_name=blob_name,path=path,file_name=file_name)

    # Check if the files are downloaded
    assert os.path.exists(os.path.join(path, blob_name, file_name)),'File does not exits'


def test_download_file_invalid_name(client):
    # Test downloading with an invalid file name
    container_name = "rawdata"
    blob_name = "raw"
    path = 'test'
    invalid_file_name=''
    with pytest.raises(ValueError) as excinfo:
        client.download_file(container_name=container_name,blob_name=blob_name,path=path,file_name=invalid_file_name)
    
    assert str(excinfo.value) == 'Invalid file name!!'


def test_upload_file_single_file(client, capsys):
    # Test uploading a single file and assert the printed message
    container_name = "rawdata"
    blob_name = "test"
    path = 'test/raw'
    file_name='cars_new.csv'


    result=client.upload_file(container_name=container_name,blob_name=blob_name,file_name=file_name,file_path=path)

    # Check if the printed message is correct
    captured = capsys.readouterr()
    expected_message = f'{file_name} uploaded to container: {container_name} successfully\n'
    assert captured.out == expected_message,' File upload failed'
    assert result==True

def test_upload_file_all_file(client, capsys):
    # Test uploading a single file and assert the printed message
    container_name = "rawdata"
    blob_name = "all_file_test"
    path = 'test/raw'

    result=client.upload_file(container_name=container_name,blob_name=blob_name,all_files=True,file_path=path)
    assert result==True,' File upload failed'

def test_upload_file_nonexistent_file(client, capsys):
    # Test uploading a non-existent file and assert the printed message
    container_name = "rawdata"
    blob_name = "test"
    path = 'test/raw'
    file_name='cars_new_non_exits.csv'


    with pytest.raises(Exception) as excinfo:
       client.upload_file(container_name=container_name,blob_name=blob_name,file_name=file_name,file_path=path)

    # Check if the proper error message is raised
    expected_output="Error while uploading the file."
    assert expected_output in str(excinfo.value)


def test_delete_file_single_file(client,capsys):
    container_name = "rawdata"
    blob_name = "test"
    file_name='cars_new.csv'
    result=client.delete_file(container_name=container_name,blob_name=blob_name,file_name=file_name)

    # Check if the printed message is correct
    captured = capsys.readouterr()
    expected_message = f'deleted from container'
    assert expected_message in str(captured.out),' File deleted failed'
    assert result==True

def test_delete_file_all_files(client,capsys):
    container_name = "rawdata"
    blob_name = "test"
    all_files=True
    result=client.delete_file(container_name=container_name,blob_name=blob_name,all_files=all_files)

    # Check if the printed message is correct
    captured = capsys.readouterr()
    expected_message = f'files deleted from container'
    assert expected_message in str(captured.out),' File deleted failed'
    assert result==True

def test_conditional_filter(client):
    container_name = "rawdata"
    blob_name = "test"
    creation_date='2023-12-01'
    files=client._conditional_filter(container_name, blob_name, creation_date, comparison='less_than')
    assert type(files)==list


def test_copy_blob_all_files(client):
    container_name='rawdata'
    blob_name='raw'
    destination_container='rawdata'
    destination_blob='test'
    all_files=True
    status=client.copy_blob(container_name=container_name,
                            blob_name=blob_name,
                            destination_container=destination_container,
                            destination_blob=destination_blob,
                            all_files=all_files)
    print(pd.DataFrame(status))
    assert type(status)==list

def test_copy_blob_single_file(client):
    container_name='rawdata'
    blob_name='raw'
    destination_container='rawdata'
    destination_blob='test'
    file_name='cars_new.csv'
    status=client.copy_blob(container_name=container_name,
                            blob_name=blob_name,
                            destination_container=destination_container,
                            destination_blob=destination_blob,
                            file_name=file_name)
    print(pd.DataFrame(status))
    assert type(status)==list

