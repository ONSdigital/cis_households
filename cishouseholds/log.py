from datetime import datetime
from datetime import timezone
from pathlib import Path

from cishouseholds.pipeline.load import get_run_id


class SplunkLogger:
    def __init__(self, log_file_path: str):
        log_file = Path(log_file_path)
        if log_file.is_file():
            self.log_file = log_file
        else:
            print(f"Warning: Splunk log file {log_file_path} does not exist, so no log will be recorded.")  # functional

    def log(self, **kwargs):
        """
        If an existing log path has been provided, logs arbitrary key value pairs to the log.
        Includes run_id and event_time in all log records.
        """
        if hasattr(self, "log_file"):
            with self.log_file.open("a") as f:
                f.write(
                    ", ".join(
                        f"{key}={value}"
                        for key, value in {
                            "run_id": get_run_id(),
                            "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                            **kwargs,
                        }.items()
                    )
                    + "\n"
                )
