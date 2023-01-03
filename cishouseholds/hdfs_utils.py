"""A collection of HDFS utils."""
import os
import subprocess

from pyspark.sql import SparkSession


def _perform(command, shell: bool = False, str_output: bool = False, ignore_error: bool = False, full_out=False):
    """
    Run shell command in subprocess returning exit code or full string output.
    _perform() will build the command that will be put into HDFS.
    This will also be used for the functions below.
    Parameters
    ----------
    shell
        If true, the command will be executed through the shell.
        See subprocess.Popen() reference.
    str_output
        output exception as string
    ignore_error
    """
    process = subprocess.Popen(command, shell=shell, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()

    if str_output:
        if stderr and not ignore_error:
            raise Exception(stderr.decode("UTF-8").strip("\n"))
        if full_out:
            return stdout
        return stdout.decode("UTF-8").strip("\n")

    return process.returncode == 0


def isfile(path: str) -> bool:
    """
    Test if file exists. Uses 'hadoop fs -test -f.

    Returns
    -------
    True for successfully completed operation. Else False.

    Note
    ----
    If checking that directory with partitioned files (i.e. csv, parquet)
    exists this will return false use isdir instead.
    """
    command = ["hadoop", "fs", "-test", "-f", path]
    return _perform(command)


def isdir(path: str) -> bool:
    """
    Test if directory exists. Uses 'hadoop fs -test -d'.

    Returns
    -------
    True for successfully completed operation. Else False.
    """
    command = ["hadoop", "fs", "-test", "-d", path]
    return _perform(command)


def create_dir(path: str) -> bool:
    """
    Create a directory including the parent directories if they don't already exist.
    Uses 'hadoop fs -mkdir -p'

    Returns
    -------
    True for successfully completed operation. Else False.
    """
    command = ["hadoop", "fs", "-mkdir", "-p", path]
    return _perform(command)


def delete_file(path: str):
    """
    Delete a file. Uses 'hadoop fs -rm'.

    Returns
    -------
    True for successfully completed operation. Else False.
    """
    command = ["hadoop", "fs", "-rm", path]
    return _perform(command)


def delete_dir(path: str):
    """
    Delete a directory. Uses 'hadoop fs -rmdir'.

    Returns
    -------
    True for successfully completed operation. Else False.
    """
    command = ["hadoop", "fs", "-rmdir", path]
    return _perform(command)


def rename(from_path: str, to_path: str, overwrite=False) -> bool:
    """
    Rename (i.e. move using full path) a file. Uses 'hadoop fs -mv'.

    Returns
    -------
    True for successfully completed operation. Else False.
    """
    # move fails if target file exists and no -f option available
    if overwrite:
        delete_file(to_path)

    command = ["hadoop", "fs", "-mv", from_path, to_path]
    return _perform(command)


def copy(from_path, to_path, overwrite=False) -> bool:
    """
    Copy a file. Uses 'hadoop fs -cp'.

    Returns
    -------
    True for successfully completed operation. Else False.
    """
    if overwrite:
        return _perform(["hadoop", "fs", "-cp", "-f", from_path, to_path])
    else:
        return _perform(["hadoop", "fs", "-cp", from_path, to_path])


def copy_local_to_hdfs(from_path: str, to_path: str) -> bool:
    """
    Move or copy a local file to HDFS.

    Parameters
    ----------
    from_path: str
        path to local file
    to_path: str
        path of where file should be placed in HDFS

    Returns
    -------
    True for successfully completed operation. Else False.
    """
    # make sure any nested directories in to_path exist first before copying
    destination_path = os.path.dirname(to_path)
    destination_path_creation = create_dir(destination_path)

    assert destination_path_creation is True, f"Unable to create destination path: {destination_path}"

    command = ["hadoop", "fs", "-copyFromLocal", from_path, to_path]
    return _perform(command)


def move_local_to_hdfs(from_path: str, to_path: str) -> bool:
    """
    Move a local file to HDFS.

    Parameters
    ----------
    from_path: str
        path to local file
    to_path: str
        path of where file should be placed in HDFS

    Returns
    -------
    True for successfully completed operation. Else False.
    """
    command = ["hadoop", "fs", "-moveFromLocal", from_path, to_path]
    return _perform(command)


def dir_size(path: str):
    """
    Get HDFS directory size.

    Returns
    -------
    str - [size] [disk space consumed] [path]

    Notes
    -----
    Hadoop replicates data for resilience, disk space consumed is size x replication.
    """
    command = ["hadoop", "fs", "-du", "-s", "-h", path]
    return _perform(command, str_output=True)


def read_header(path: str):
    """
    Reads the first line of a file on HDFS
    """
    return _perform(f"hadoop fs -cat {path} | head -1", shell=True, str_output=True, ignore_error=True)


def write_string_to_file(content: bytes, path: str):
    """
    Writes a string into the specified file path
    """
    _write_string_to_file = subprocess.Popen(f"hadoop fs -put - {path}", stdin=subprocess.PIPE, shell=True)
    return _write_string_to_file.communicate(content)


def read_file_to_string(path: str, full_out: bool = False):
    """
    Reads file into a string
    """
    command = ["hadoop", "fs", "-cat", path]
    return _perform(command, str_output=True, full_out=full_out)


def hdfs_stat_size(path: str):
    """
    Runs stat command on a file or directory to get the size in bytes.
    """
    command = ["hadoop", "fs", "-du", "-s", path]
    return _perform(command, str_output=True).split(" ")[0]


def hdfs_md5sum(path: str):
    """
    Get md5sum of a specific file on HDFS.
    """
    return _perform(f"hadoop fs -cat {path} | md5sum", shell=True, str_output=True, ignore_error=True).split(" ")[0]


def cleanup_checkpoint_dir(spark: SparkSession):
    """Cleanup checkpoint files at the the end of the job

    >>> from pyspark.sql import SparkSession
    >>> spark = SparkSession.builder.getOrCreate()
    >>> # try deleting a checkpoint when it is not set
    >>> cleanup_checkpoint_dir(spark)  # doesn't throw an error
    Checkpoint directory not set
    >>> # Set-up a check point
    >>> spark.sparkContext.setCheckpointDir('D:/projects/checkpoints')
    >>> cleanup_checkpoint_dir(spark)
    Found checkpoint directory: file:/D:/projects/checkpoints/e5b71b89-402b-4035-a481-ce83c688c2d3
    Deleted checkpoint directory: file:/D:/projects/checkpoints/e5b71b89-402b-4035-a481-ce83c688c2d3

    """

    # get sparkContext from the spark session object
    sc = spark.sparkContext

    # find out the checkpoint dir associated with the spark session
    my_checkpoint_dir = sc._jsc.sc().getCheckpointDir()

    # check if checkpoint directory has actually been set
    if my_checkpoint_dir.isEmpty():
        print("Checkpoint directory not set")  # functional
        return None

    folder_to_delete = my_checkpoint_dir.get()
    print(f"Found checkpoint directory: {folder_to_delete}")  # functional

    # get a Hadoop filesystem handle
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())

    # make sure the folder exists before deleting it
    if fs.exists(sc._jvm.org.apache.hadoop.fs.Path(folder_to_delete)):
        fs.delete(sc._jvm.org.apache.hadoop.fs.Path(folder_to_delete))
        print(f"Deleted checkpoint directory: {folder_to_delete}")  # functional

    return None
