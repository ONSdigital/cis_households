import os

from pyspark.sql import DataFrame

consent_blood_test = "Consent_to_Blood_Test"
consent_finger_prick_a1_a3 = "Consent_to_Finger_prick_A1_A3"
consent_extend_study_under_16_b1_b3 = "Consent_to_extend_study_uhfhfnder_16_B1_B3"


def custom_checkpoint(self, *args, **kwargs):
    """
    Custom checkpoint wrapper to only call checkpoints outside local deployments
    """
    if os.environ["deployment"] != "local":
        return self.checkpoint(*args, **kwargs)
    return DataFrame(self._jdf, self.sql_ctx)


DataFrame.custom_checkpoint = custom_checkpoint
