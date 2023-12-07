from helper.Constant import *


def timestamp_to_str(timestamp):
    """Converts provided datetime object to string with format as yyyy-mm-dd hh24:mm:ss

    :param timestamp: datetime object to be converted
    :return: string in format yyyy-mm-dd hh24:mm:ss. In case of None object, returns None
    """
    if timestamp is not None:
        return timestamp.strftime(CONSTANT.TIMESTAMP_FORMAT)
    else:
        return timestamp


def timestamp_to_str_format(timestamp, format):
    """Converts provided datetime object to string with specified format

    :param timestamp: datetime object to be converted
    :param format: string format to which datetime object should be converted
    :return: string of datetime object converted in specified format. In case of None object, returns None
    """
    if timestamp is not None:
        return timestamp.strftime(format)
    else:
        return timestamp
