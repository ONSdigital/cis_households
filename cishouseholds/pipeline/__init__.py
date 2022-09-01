import os

from pyspark.sql import DataFrame


def custom_checkpoint(self, *args, **kwargs):
    """
    Custom checkpoint wrapper to only call checkpoints outside local deployments
    """
    if os.environ["deployment"] != "local":
        return self.checkpoint(*args, **kwargs)
    return DataFrame(self._jdf, self.sql_ctx)


DataFrame.custom_checkpoint = custom_checkpoint
