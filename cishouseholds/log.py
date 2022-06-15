from datetime import datetime
from datetime import timezone

from cishouseholds.pipeline.load import get_run_id


class SplunkLogger:
    def __init__(self, log_file_path: str):
        self.log_file_path = log_file_path

    def log(self, **kwargs):
        with open(self.log_file_path, "a") as f:
            f.write(
                ", ".join(
                    f"{key}={value}"
                    for key, value in {
                        **{
                            "run_id": get_run_id(),
                            "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                        },
                        **kwargs,
                    }
                )
            )
