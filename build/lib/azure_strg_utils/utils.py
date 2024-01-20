from datetime import time
import fnmatch
import operator
import pandas as pd

def _save_dataframe(file_content,dataframe,format):
    """
    Save a Pandas DataFrame to a specified file format.

    \n Args:
        - file_content (io.BytesIO): A BytesIO object to store the content of the file.
        - dataframe (pd.DataFrame): The Pandas DataFrame to be saved.
        - format (str): The desired file format ('csv', 'json', or 'xml').

    \n Raises:
        - Exception: If an error occurs during the saving process.
    """
    try:
        if format=='csv':
            dataframe.to_csv(file_content,index=False)
        elif format=='json':
            dataframe.to_json(file_content,orient='records')
        elif format=='xml':
            dataframe.to_xml(file_content)
        else:
            dataframe.to_csv(file_content,index=False)
    except Exception as e:
        raise e

def _abort_copy(blob_client,abort_time):
    """
    Abort a copy operation for a blob client if it takes longer than a specified duration.
    \n Args:
        - blob_client: Blob client object representing the blob to monitor.
        - abort_time (int): Time duration (in seconds) to monitor the copy operation and abort if necessary.

    \n Raises:
        - Exception: Raises an exception if an error occurs during the abort operation.
    """
    try:
        for i in range(abort_time):
            status=blob_client.get_blob_properties().copy.status
            print("Copy status: " + status)
            if status=='success':
                return True
            time.sleep(1)

        if status!='success':
            props=blob_client.get_blob_properties()
            print(props.copy.status)
            copy_id=props.copy.id

            # abort the copy
            blob_client.abort_copy(copy_id)
            props = blob_client.get_blob_properties()
            print(props.copy.status)
            return False
    except Exception as e:
        raise e
    

def _filter_file(file_regex,file_list):
    """
    Filter the list of files based on the provided regex pattern.
    \n Args:
        - file_regex (str): The regex expression used to filter the files.
        - file_list (list): List of file names to be filtered.
    \n Returns:
        - list: A filtered list of file names based on the regex pattern.
    """
    if file_regex!=None and not isinstance(file_regex, int):
        file_list = fnmatch.filter(file_list, file_regex)
    else:
        file_list=file_list
    return file_list



def _comparison_operator(comparison):
        """
        Returns the comparison operator based on the provided comparison type.
        \n Args:
            - comparison (str): The type of comparison operator required. Possible values are:
                            - 'less_than': Less than comparison operator (<).
                            - 'less_than_or_equal': Less than or equal to comparison operator (<=).
                            - 'greater_than': Greater than comparison operator (>).
                            - 'greater_than_or_equal': Greater than or equal to comparison operator (>=).
                            - Any other value will return an equal to comparison operator (==).

        \n Returns:
            - function: The comparison operator function based on the specified comparison type.
        """
        if comparison == 'less_than':
            return operator.lt
        elif comparison == 'less_than_or_equal':
            return operator.le
        elif comparison == 'greater_than':
            return operator.gt
        elif comparison == 'greater_than_or_equal':
            return operator.ge
        else:
            return operator.eq


def _read_file(file_name,stream):
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