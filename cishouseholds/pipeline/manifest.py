import json
import os
from datetime import datetime

from cishouseholds.hdfs_utils import delete_file
from cishouseholds.hdfs_utils import hdfs_md5sum
from cishouseholds.hdfs_utils import hdfs_stat_size
from cishouseholds.hdfs_utils import isdir
from cishouseholds.hdfs_utils import isfile
from cishouseholds.hdfs_utils import read_header
from cishouseholds.hdfs_utils import write_string_to_file


class ManifestError(Exception):
    pass


class Manifest:
    """
    An outgoing file transfer manifest. Accumulates file metadata before writing a JSON manifest file.

    Used by Apache NiFi for data integrity checks before initiating data transfers.

    Attributes
    ==========
    outgoing_directory
        location to write files to
    pipeline_run_datetime
        datetime of the current pipeline run, used to version outputs
    dry_run
        when True, cleans up output files after a successful run
    """

    def __init__(self, outgoing_directory: str, pipeline_run_datetime: datetime, dry_run: bool = False):
        self.outgoing_directory = outgoing_directory
        if not isdir(outgoing_directory):
            raise ManifestError(f"Outgoing directory does not exist: {self.outgoing_directory}")

        if not isinstance(pipeline_run_datetime, datetime):
            raise ManifestError("Pipeline run datetime must be a datetime object.")

        self.manifest_datetime = pipeline_run_datetime.strftime("%Y%m%d_%H%M%S")

        self.manifest_file_path = os.path.join(outgoing_directory, (self.manifest_datetime + "_manifest.json"))
        self.manifest: dict = {"files": []}
        self.written = False

        self.invalid_headers: list = []
        self.dry_run = dry_run

    def add_file(
        self, relative_file_path: str, column_header: str, validate_col_name_length: bool = True, sep: str = "|"
    ):
        """
        Add a file in the outgoing folder to the manifest.
        The file must exist in a subdirectory of the manifest `outgoing_directory`.

        Parameters
        ----------
        relative_file_path
            from outgoing directory to the file that you want to add to the manifest
        column_header
            the exact column header string
        """
        if ".." in relative_file_path:
            raise ManifestError(f"File must be in a subdirectory of the outgoing directory: {relative_file_path}")

        absolute_file_path = os.path.join(self.outgoing_directory, relative_file_path)

        if not isfile(absolute_file_path):
            raise ManifestError(f"Cannot add file to manifest, file does not exist: {absolute_file_path}")

        true_header_string = read_header(absolute_file_path)
        true_header_list = true_header_string.split(sep)
        if true_header_string != column_header:
            column_header_list = column_header.split(sep)

            self.invalid_headers.append(
                f"File:{absolute_file_path}\n"
                f"Expected:     {column_header}\n"
                f"Got:          {true_header_string}\n"
                f"Missing:      {set(column_header_list) - set(true_header_list)}\n"
                f"Additional:   {set(true_header_list) - set(column_header_list)}\n"
            )

        if validate_col_name_length:
            col_above_max_len = [head for head in true_header_list if len(head) > 32]

            if len(col_above_max_len) > 0:
                self.invalid_headers.append(
                    f"File:{absolute_file_path}\n"
                    f"These column names are exceeding the maximum char length of 32: {col_above_max_len}\n"
                )

        file_manifest = {
            "file": os.path.basename(relative_file_path),
            "subfolder": os.path.dirname(relative_file_path),
            "sizeBytes": hdfs_stat_size(absolute_file_path),
            "md5sum": hdfs_md5sum(absolute_file_path),
            "header": column_header,
        }
        self.manifest["files"].append(file_manifest)

    def write_manifest(self):
        """
        Write outgoing file manifest to JSON in HDFS.
        A manifest can only be written once during pipeline run.
        """
        any_invalid_headers = len(self.invalid_headers) > 0

        if any_invalid_headers:
            self._delete_files_after_fail()
            raise ManifestError("\n".join(self.invalid_headers))

        if self.written:
            raise ManifestError("Manifest has already been written.")

        if len(self.manifest["files"]) < 1:
            raise ManifestError("Can't write an empty Manifest.")

        if self.dry_run:
            self._delete_files_after_fail()
            self.written = True
            return

        write_string_to_file(json.dumps(self.manifest, indent=4).encode("utf-8"), self.manifest_file_path)
        self.written = True

    def _delete_files_after_fail(self):
        """
        Delete all files in the manifest. Used when manifest content does not match the target file for transfer,
        or during a dry run.
        """

        for f in self.manifest["files"]:
            absolute_path = os.path.join(self.outgoing_directory, f["subfolder"], f["file"])
            if isfile(absolute_path):
                delete_file(absolute_path)
