from typing import Optional, List, Any


def format_date(date: Optional[str]) -> Optional[str]:
    """
    Append milliseconds to a date string.

    - If `date` is None or empty, return it as is.
    - If the string is 19 characters long, append '.000'.
    - Otherwise, pad the string to 23 characters with '0'.
    """
    return date if not date else f"{date}.000" if len(date) == 19 else date.ljust(23, "0")


def format_tstamp(tstamp: str) -> str:
    """
    To correct a timestamp

    Args:
        tstamp (str): the tstamp.
        
    Returns:
        tstamp (str): The correct tstamp.
    """

    return "0x" + tstamp.upper() if tstamp[:2] != '0x' and len(tstamp) == 16 else "0x" + tstamp[2:].upper()


def remove_fields(record: dict, fields_to_keep: list) -> dict:
    """
    A function to remove useless fileds in the records

    Args:
        data (list): The data with useless fields.
        
    Returns:
        data (list): The data only with selected files.
    """

    return {key: value for key, value in record.items() if key in fields_to_keep}


def standard_date(record: dict, fields_to_format: dict):
    """
    Standarize the datetime fiels in all records.

    Args:
        data (list): The data without standard datetime files.
        
    Returns:
        data (list): The standard data.
    """
    
    for field in fields_to_format:
        record[field] = standard_date(record[field])
        
    record['tstamp'] = standard_date(record['tstamp'])
    return record


def preprocess_data(data: list, fields_to_keep: list) -> list:
    """
    A function to pre processing the data.

    Args:
        data (list): The data without pre processing.
        fields_to_keep (list): field to keep in all records
        
    Returns:
        data (list): The preprocessed data.
    """
    return [remove_fields(x, fields_to_keep) for x in data]


# def standard_date(record: dict) -> dict:
#     """
#     Standarize the datetime fiels in all records.

#     Args:
#         data (list): The data without standard datetime files.
        
#     Returns:
#         data (list): The standard data.
#     """

#     record['transDate']     = format_date(record['transDate'])
#     record['winlostdate']   = format_date(record['winlostdate'])
#     record['modifyDate']    = format_date(record['modifyDate'])
#     record['tstamp']        = format_date(record['tstamp'])
#     return record