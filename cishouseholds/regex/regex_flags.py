from pyspark.sql import functions as F

from cishouseholds.expressions import any_column_null


def flag_records_for_work_location_null() -> F.Column:
    """Flag records for application of "Work location" rules. Rule_id: 7000"""
    return (
        F.col("work_location").isNull() | (F.col("work_main_job_title").isNull() & F.col("work_main_job_role").isNull())
    ) & (
        F.col("work_status_v0").isin(
            "Furloughed (temporarily not working)",
            "Not working (unemployed, retired, long-term sick etc.)",
            "Student",
        )
    )


def flag_records_for_work_location_student() -> F.Column:
    """Flag records for application of "Work location" rules for students. Rule_id: 7000"""
    return F.col("work_status_v0").isin("Student") | (F.col("age_at_visit") < F.lit(16))


def flag_records_for_work_from_home_rules() -> F.Column:
    """Flag records for application of "Work From Home" rules. Rule_id: 1000"""
    return F.col("work_location").isNull() & ~(
        (
            F.col("work_status_v0").isin(
                "Furloughed (temporarily not working)",
                "Not working (unemployed, retired, long-term sick etc.)",
                "Student",
            )
        )
        | (F.col("age_at_visit") < F.lit(16))
    )


def flag_records_for_furlough_rules_v0() -> F.Column:
    """Flag records for application of "Furlough Rules V0" rules. Rule_id: 2000"""
    return F.col("work_status_v0").isin(
        "Employed",
        "Self-employed",
        "Not working (unemployed, retired, long-term sick etc.)",
    )


def flag_records_for_furlough_rules_v1_a() -> F.Column:
    """Flag records for application of "Furlough Rules V1-a" rules. Rule_id: 2001"""
    return F.col("work_status_v1").isin(
        "Employed and currently working",
        "Looking for paid work and able to start",
        "Not working and not looking for work",
    )


def flag_records_for_furlough_rules_v1_b() -> F.Column:
    """Flag records for application of "Furlough Rules V1-b" rules. Rule_id: 2002"""
    return F.col("work_status_v1").isin("Self-employed and currently working")


def flag_records_for_furlough_rules_v2_a() -> F.Column:
    """Flag records for application of "Furlough Rules V2-a" rules. Rule_id: 2003"""
    return F.col("work_status_v2").isin(
        "Employed and currently working",
        "Looking for paid work and able to start",
        "Not working and not looking for work",
    )


def flag_records_for_furlough_rules_v2_b() -> F.Column:
    """Flag records for application of "Furlough Rules V2-b" rules. Rule_id: 2004"""
    return F.col("work_status_v2").isin("Self-employed and currently working")


def flag_records_for_self_employed_rules_v0() -> F.Column:
    """Flag records for application of "Self-employed Rules V0" rules. Rule_id: 3000"""
    return F.col("work_status_v0").isNull() | F.col("work_status_v0").isin("Employed")


def flag_records_for_self_employed_rules_v1_a() -> F.Column:
    """Flag records for application of "Self-employed Rules V1-a" rules. Rule_id: 3001"""
    return F.col("work_status_v1").isin("Employed and currently working")


def flag_records_for_self_employed_rules_v1_b() -> F.Column:
    """Flag records for application of "Self-employed Rules V1-b" rules. Rule_id: 3002"""
    return F.col("work_status_v1").isin("Employed and currently not working")


def flag_records_for_self_employed_rules_v2_a() -> F.Column:
    """Flag records for application of "Self-employed Rules V2-a" rules. Rule_id: 3003"""
    return F.col("work_status_v2").isin("Employed and currently working")


def flag_records_for_self_employed_rules_v2_b() -> F.Column:
    """Flag records for application of "Self-employed Rules V2-b" rules. Rule_id: 3004"""
    return F.col("work_status_v2").isin("Employed and currently not working")


def flag_records_for_retired_rules() -> F.Column:
    """Flag records for application of "Retired" rules. Rule_id: 4000, 4001, 4002"""
    return (
        any_column_null(["work_status_v0", "work_status_v1", "work_Status_v2"])
        & F.col("work_main_job_title").isNull()
        & F.col("work_main_job_role").isNull()
        & (F.col("age_at_visit") > F.lit(75))
    )


def flag_records_for_not_working_rules_v0() -> F.Column:
    """Flag records for application of "Not working Rules V0" rules. Rule_id: 5000"""
    return F.col("work_status_v0").isin("Employed", "Self-employed")


def flag_records_for_not_working_rules_v1_a() -> F.Column:
    """Flag records for application of "Not working Rules V1-a" rules. Rule_id: 5001"""
    return F.col("work_status_v1").isin("Employed and currently working")


def flag_records_for_not_working_rules_v1_b() -> F.Column:
    """Flag records for application of "Not working Rules V1-b" rules. Rule_id: 5002"""
    return F.col("work_status_v1").isin("Self-employed and currently working")


def flag_records_for_not_working_rules_v2_a() -> F.Column:
    """Flag records for application of "Not working Rules V2-a" rules. Rule_id: 5003"""
    return F.col("work_status_v2").isin("Employed and currently working")


def flag_records_for_not_working_rules_v2_b() -> F.Column:
    """Flag records for application of "Not working Rules V2-b" rules. Rule_id: 5004"""
    return F.col("work_status_v2").isin("Self-employed and currently working")


def flag_records_for_school_v2_rules() -> F.Column:
    """Flag records for application of "School rules -v2" rules. Rule_id: 6000, 6002, 6005"""
    return ((F.col("age_at_visit") >= F.lit(4)) & (F.col("age_at_visit") <= F.lit(18))) & ~(
        F.col("school_year").isNull()
    )


def flag_records_for_uni_v0_rules() -> F.Column:
    """Flag records for application of "Uni-v0" rules. Rule_id: 6000"""
    return (
        (F.col("age_at_visit") >= F.lit(17))
        & (
            F.col("work_status_v0").isNull()
            | F.col("work_status_v0").isin(
                "Furloughed (temporarily not working)",
                "Not working (unemployed, retired, long-term sick etc.)",
            )
        )
    ) | ((F.col("age_at_visit") >= F.lit(17)) & (F.col("age_at_visit") < F.lit(22)))


def flag_records_for_uni_v1_rules() -> F.Column:
    """Flag records for application of "Uni-v0" rules. Rule_id: 6002"""
    return (
        (F.col("age_at_visit") >= F.lit(17))
        & (
            F.col("work_status_v1").isNull()
            | F.col("work_status_v1").isin(
                "Employed and currently not working",
                "Self-employed and currently not working",
                "Looking for paid work and able to start",
                "Not working and not looking for work",
                "Retired",
            )
        )
    ) | ((F.col("age_at_visit") >= F.lit(19)) & (F.col("age_at_visit") < F.lit(22)))


def flag_records_for_uni_v2_rules() -> F.Column:
    """Flag records for application of "Uni-v2" rules. Rule_id: 6007"""
    return (
        (F.col("age_at_visit") >= F.lit(17))
        & (
            F.col("work_status_v2").isNull()
            | F.col("work_status_v2").isin(
                "Employed and currently not working",
                "Self-employed and currently not working",
                "Looking for paid work and able to start",
                "Not working and not looking for work",
                "Retired",
            )
        )
    ) | ((F.col("age_at_visit") >= F.lit(19)) & (F.col("age_at_visit") < F.lit(22)))


def flag_records_for_college_v0_rules() -> F.Column:
    """Flag records for application of "College-v0" rules. Rule_id: 6000"""
    return (
        (F.col("age_at_visit") >= F.lit(16))
        & (
            F.col("work_status_v0").isNull()
            | F.col("work_status_v0").isin(
                "Furloughed (temporarily not working)",
                "Not working (unemployed, retired, long-term sick etc.)",
            )
        )
    ) | ((F.col("age_at_visit") >= F.lit(17)) & (F.col("age_at_visit") < F.lit(19)))


def flag_records_for_college_v1_rules() -> F.Column:
    """Flag records for application of "College-v0" rules. Rule_id: 6002"""
    return (
        (F.col("age_at_visit") >= F.lit(16))
        & (
            F.col("work_status_v1").isNull()
            | F.col("work_status_v1").isin(
                "Employed and currently not working",
                "Self-employed and currently not working",
                "Looking for paid work and able to start",
                "Not working and not looking for work",
                "Retired",
            )
        )
    ) | ((F.col("age_at_visit") >= F.lit(17)) & (F.col("age_at_visit") < F.lit(19)))


def flag_records_for_college_v2_rules() -> F.Column:
    """Flag records for application of "College-v2" rules. Rule_id: 6006"""
    return (
        (F.col("age_at_visit") >= F.lit(16))
        & (
            F.col("work_status_v2").isNull()
            | F.col("work_status_v2").isin(
                "Employed and currently not working",
                "Self-employed and currently not working",
                "Looking for paid work and able to start",
                "Not working and not looking for work",
                "Retired",
            )
        )
    ) | ((F.col("age_at_visit") >= F.lit(17)) & (F.col("age_at_visit") < F.lit(19)))


def flag_records_for_childcare_v0_rules() -> F.Column:
    """Flag records for application of "Childcare-V0" rules. Rule_id: 6001"""
    return (
        (F.col("age_at_visit") < F.lit(4))
        & F.col("school_year").isNull()
        & ~(
            F.col("work_status_v0").isin(
                "Furloughed (temporarily not working)",
                "Not working (unemployed, retired, long-term sick etc.)",
            )
        )
    )


def flag_records_for_childcare_v1_rules() -> F.Column:
    """Flag records for application of "Childcare-V1" rules. Rule_id: 6003"""
    return (
        (F.col("age_at_visit") <= F.lit(4))
        & F.col("school_year").isNull()
        & ~(
            F.col("work_status_v1").isin(
                "Child under 5y not attending child care", "Child under 5y attending child care"
            )
        )
    )


def flag_records_for_childcare_v2_b_rules() -> F.Column:
    """Flag records for application of "Childcare-V2_b" rules. Rule_id: 6004"""
    return (
        (F.col("age_at_visit") <= F.lit(5))
        & F.col("school_year").isNull()
        & ~(
            F.col("work_status_v2").isin(
                "Child under 4-5y not attending child care",
                "Child under 4-5y attending child care",
            )
        )
    )
