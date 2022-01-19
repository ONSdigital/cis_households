"""A collection of HDFS utils."""
import re
import subprocess
from typing import List
from typing import Optional


def _perform(command, str_ouput=False):
    """Run shell command in subprocess returning exit code or full string output."""
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()

    if str_ouput:
        if stderr:
            raise Exception(stderr.decode("UTF-8").strip("\n"))
        return stdout.decode("UTF-8").strip("\n")

    return process.returncode


def isfile(path):
    """
    Test if file exists. Uses 'hadoop fs -test -f.

    Args: path (String)

    Returns:
        bool: Returns True for successfully completed operation. Else False.

    Note:
        If checking that directory with partitioned files (i.e. csv, parquet)
        exists this will return false use isdir instead.
    """
    command = ["hadoop", "fs", "-test", "-f", path]
    return _perform(command)


def isdir(path):
    """
    Test if directory exists. Uses 'hadoop fs -test -d'.

    Args: path (String)

    Returns:
        bool: Returns True for successfully completed operation. Else False.
    """
    command = ["hadoop", "fs", "-test", "-d", path]
    return _perform(command)


def create_dir(path):
    """
    Create a directory. Uses 'hadoop fs -mkdir'.

    Args: path (String)

    Returns:
        bool: Returns True for successfully completed operation. Else False.
    """
    command = ["hadoop", "fs", "-mkdir", path]
    return _perform(command)


def delete_file(path):
    """
    Delete a file. Uses 'hadoop fs -rm'.

    Args: path (String)

    Returns:
        bool: Returns True for successfully completed operation. Else False.
    """
    command = ["hadoop", "fs", "-rm", path]
    return _perform(command)


def delete_dir(path):
    """
    Delete a directory. Uses 'hadoop fs -rmdir'.

    Args: path (String)

    Returns:
        bool: Returns True for successfully completed operation. Else False.
    """
    command = ["hadoop", "fs", "-rmdir", path]
    return _perform(command)


def rename(from_path, to_path, overwrite=False):
    """
    Rename (i.e. move using full path) a file. Uses 'hadoop fs -mv'.

    Args: path (String)

    Returns:
        bool: Returns True for successfully completed operation. Else False.
    """
    # move fails if target file exists and no -f option available
    if overwrite:
        delete_file(to_path)

    command = ["hadoop", "fs", "-mv", from_path, to_path]
    return _perform(command)


def copy(from_path, to_path, overwrite=False):
    """
    Copy a file. Uses 'hadoop fs -cp'.

    Args: path (String)

    Returns:
        bool: Returns True for successfully completed operation. Else False.
    """
    if overwrite:
        return _perform(["hadoop", "fs", "-cp", "-f", from_path, to_path])
    else:
        return _perform(["hadoop", "fs", "-cp", from_path, to_path])


def copy_local_to_hdfs(from_path, to_path):
    """
    Move or copy a local file to HDFS.

    Args:
        from_path (String): path to local file
        to_path (String): path of where file should be placed in HDFS

    Returns:
        bool: Returns True for successfully completed operation. Else False.
    """
    command = ["hadoop", "fs", "-copyFromLocal", from_path, to_path]
    return _perform(command)


def move_local_to_hdfs(from_path, to_path):
    """
    Move a local file to HDFS.

    Args:
        from_path (String): path to local file
        to_path (String): path of where file should be placed in HDFS

    Returns:
        bool: Returns True for successfully completed operation. Else False.
    """
    command = ["hadoop", "fs", "-moveFromLocal", from_path, to_path]
    return _perform(command)


def dir_size(path):
    """
    Get HDFS directory size.

    Args:
        path (String): path to HDFS directory

    Returns:
        str - [size] [disk space consumed] [path]
        Hadoop replicates data for resilience, disk space consumed is size x replication.
    """
    command = ["hadoop", "fs", "-du", "-s", "-h", path]
    return _perform(command, True)


def read_header(path):
    """
    Reads the first line of a file on HDFS
    """
    command = ["hadoop", "fs", "-cat", path, "|", "head"]
    return _perform(command, True)


def write_string_to_file(content: str, path):
    """
    Writes a string into the specified file path
    """
    command = ["echo", content, "|", "hadoop", "fs", "-put", "-", path]
    return _perform(command, True)


def read_file_to_string(path):
    """
    Reads file into a string
    """
    command = ["hadoop", "fs", "-cat", path]
    return _perform(command, True)
