import csv


class InvalidFileError(Exception):
    pass


def validate_csv_fields(csv_file: str, delimiter: str = ","):
    """
    Function to validate the number of fields within records of a csv file.

    Parameters
    ----------
    csv_file
        File path for csv file to be validated
    delimiter
        Delimiter used in csv file, default as ','
    """
    row_errors = []
    with open(csv_file) as f:
        reader = csv.reader(f, delimiter=delimiter)
        n_fields = len(next(reader))

        for line_num, row in enumerate(reader):
            row_fields = len(row)
            if row_fields != n_fields:
                row_errors.append(f"{line_num+1}")

    if row_errors:
        raise InvalidFileError(
            f"Expected number of fields in each row is {n_fields}",
            f"Rows not matching this are: {', '.join(row_errors)}",
        )
    return True


def validate_csv_header(csv_file: str, expected_header: str, delimiter: str = ","):
    """
    Function to validate header in csv file matches expected header.

    Parameters
    ----------
    csv_file
        File path for csv file to be validated
    expected_header
        Exact header expected in csv file
    delimiter
        Delimiter used in csv file, default as ','
    """

    with open(csv_file) as f:
        header = f.readline()

    if expected_header is not None:
        is_match = expected_header == header
        if is_match is False:
            raise InvalidFileError(
                f"Header of csv file {csv_file} does not match expected header",
                f"Actual header: {header}",
                f"Expected header: {expected_header}",
            )
    return True


if __name__ == "__main__":
    pass
