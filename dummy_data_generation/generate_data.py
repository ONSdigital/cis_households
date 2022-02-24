"""
Generate fake data for households survey raw input data.
"""
# mypy: ignore-errors
from datetime import datetime
from datetime import timedelta
from io import BytesIO
from pathlib import Path

import pandas as pd
from mimesis.schema import Field
from mimesis.schema import Schema

from cishouseholds.hdfs_utils import write_string_to_file
from cishouseholds.pipeline.load import get_full_table_name
from cishouseholds.pipeline.pipeline_stages import register_pipeline_stage
from cishouseholds.pyspark_utils import get_or_create_spark_session
from dummy_data_generation.helpers import code_mask
from dummy_data_generation.helpers import CustomRandom
from dummy_data_generation.helpers_weight import Distribution
from dummy_data_generation.schemas import get_blood_data_description
from dummy_data_generation.schemas import get_historical_blood_data_description
from dummy_data_generation.schemas import get_nims_data_description
from dummy_data_generation.schemas import get_swab_data_description
from dummy_data_generation.schemas import get_unassayed_blood_data_description
from dummy_data_generation.schemas import get_voyager_0_data_description
from dummy_data_generation.schemas import get_voyager_1_data_description
from dummy_data_generation.schemas import get_voyager_2_data_description


_ = Field("en-gb", seed=42, providers=[Distribution, CustomRandom])


def write_output(pd_df: pd.DataFrame, filepath: str, sep: str = ","):
    output = BytesIO()
    pd_df.to_csv(output, index=False, sep=sep)
    write_string_to_file(output.getbuffer(), filepath)


def generate_survey_v0_data(directory, file_date, records, swab_barcodes, blood_barcodes):
    """
    Generate survey v0 data.
    """
    schema = Schema(
        schema=get_voyager_0_data_description(_, blood_barcodes=blood_barcodes, swab_barcodes=swab_barcodes)
    )
    survey_responses = pd.DataFrame(schema.create(iterations=records))
    write_output(survey_responses, directory / f"ONS_Datafile_{file_date}.csv", "|")
    return survey_responses


def generate_survey_v1_data(directory, file_date, records, swab_barcodes, blood_barcodes):
    """
    Generate survey v1 data.
    """
    schema = Schema(
        schema=get_voyager_1_data_description(_, blood_barcodes=blood_barcodes, swab_barcodes=swab_barcodes)
    )
    survey_responses = pd.DataFrame(schema.create(iterations=records))

    write_output(survey_responses, directory / f"ONSECRF4_Datafile_{file_date}.csv", "|")
    return survey_responses


def generate_survey_v2_data(directory, file_date, records, swab_barcodes, blood_barcodes):
    """
    Generate survey v2 data.
    """
    schema = Schema(
        schema=get_voyager_2_data_description(_, blood_barcodes=blood_barcodes, swab_barcodes=swab_barcodes)
    )
    survey_responses = pd.DataFrame(schema.create(iterations=records))
    write_output(survey_responses, directory / f"ONSECRF5_Datafile_{file_date}.csv", "|")
    return survey_responses


def generate_ons_gl_report_data(directory, file_date, records):

    """
    Generate dummy swab test results.
    """
    schema = Schema(schema=get_swab_data_description(_))
    survey_ons_gl_report = pd.DataFrame(schema.create(iterations=records))

    write_output(survey_ons_gl_report, directory / f"ONS_GL_Report_{file_date}_0000.csv")


def generate_unioxf_medtest_data(directory, file_date, records):
    """
    Generate Oxford blood test data.
    """
    s_gene_description = get_blood_data_description(_, "S")
    s_schema = Schema(schema=s_gene_description)
    survey_unioxf_medtest_s = pd.DataFrame(s_schema.create(iterations=records))

    n_gene_description = get_blood_data_description(_, "N")
    n_schema = Schema(schema=n_gene_description)
    survey_unioxf_medtest_n = pd.DataFrame(n_schema.create(iterations=records))

    for row in range(0, records):
        if _("integer_number", start=0, end=100) > 15:
            for col in ["Serum Source ID", "Well ID"]:
                survey_unioxf_medtest_n.at[row, col] = survey_unioxf_medtest_s.at[row, col]
            survey_unioxf_medtest_n.at[row, "Plate Barcode"] = (
                survey_unioxf_medtest_s.at[row, "Plate Barcode"][:-3]
                + "N"
                + survey_unioxf_medtest_s.at[row, "Plate Barcode"][-2:]
            )

    write_output(survey_unioxf_medtest_s, directory / f"Unioxf_medtestS_{file_date}.csv")
    write_output(survey_unioxf_medtest_n, directory / f"Unioxf_medtestN_{file_date}.csv")


def generate_historic_bloods_data(directory, file_date, records, target):
    """
    Generate historic bloods file
    """
    schema = Schema(schema=get_historical_blood_data_description(_))
    historic_bloods_data = pd.DataFrame(schema.create(iterations=records))

    write_output(historic_bloods_data, directory / f"historical_bloods_{target}_{file_date}.csv")


def generate_unassayed_bloods_data(directory, file_date, records):
    """
    generate unassayed bloods data
    """
    schema = Schema(schema=get_unassayed_blood_data_description(_))
    unassayed_bloods_data = pd.DataFrame(schema.create(iterations=records))

    write_output(unassayed_bloods_data, directory / f"Unioxf_medtest_unassayed_{file_date}.csv")


def generate_northern_ireland_data(directory, file_date, records):
    """
    generate northern ireland file.
    """
    northern_ireland_data_description = lambda: {  # noqa: E731
        "UIC": _("random.custom_code", mask="############", digit="#"),
        "Sample": _("random.custom_code", mask="#&&&", digit="#", char="&"),
        "oa11": code_mask(mask="N0000####", min_code=["N00000001", None], max_code=["N00004537", None]),
        "laua": code_mask(mask="N090000##", min_code=["N09000001", None], max_code=["N09000011", None]),
        "ctry": "N92000002",
        "GOR9D": "N99999999",
        "lsoa11": code_mask(mask="95&&##S#", min_code=["95AA01S1", None], max_code=["95ZZ16S2", None]),
        "msoa11": "N99999999",
        "oac11": code_mask(mask="#&#", min_code=["1A1", None], max_code=["8B3", None], use_incremntal_letters=True),
        "CIS20CD": code_mask(mask="J06000###", min_code="J06000229", max_code="J06000233"),
        "rgn": "N92000002",
        "imd": code_mask(mask="00###", min_code=["00001", None], max_code=["00890", None]),
        "interim_id": 999,
    }

    schema = Schema(schema=northern_ireland_data_description)
    northern_ireland_data = pd.DataFrame(schema.create(iterations=records))

    write_output(northern_ireland_data, directory / f"CIS_Direct_NI_{file_date}.csv")


def generate_sample_direct_data(directory, file_date, records):
    """
    generate sample direct eng data
    """
    sample_direct_eng_description = lambda: {  # noqa: E731
        "UAC": _("random.custom_code", mask="############", digit="#"),
        "LA_CODE": _("random.custom_code", mask="&########", digit="#"),
        "Bloods": _("choice", items=["Swab only", "Swab and Blood"]),
        "oa11": code_mask(
            mask="X00######",
            min_code=["E00000001", "W00000001", "S00088956", None],
            max_code=["E00176774", "W00010265", "S00135306", None],
        ),
        "laua": code_mask(
            mask="X########",
            min_code=["E06000001", "E07000008", "E08000001", "E09000001", "W06000001", "S12000005", None],
            max_code=["E06000062", "E07000246", "E08000037", "E09000033", "W06000024", "S12000050", None],
        ),
        "ctry": _("choice", items=["E92000001", "W92000004", "S92000003"]),
        "CUSTODIAN_REGION_CODE": _(
            "choice",
            items=[
                code_mask(mask="E########", min_code="E12000001", max_code="E12000009"),
                "W99999999 ",
                "S99999999",
                None,
            ],
        ),
        "lsoa11": code_mask(
            mask="E########",
            min_code=["E01000001", "W01000001", "S01006506", None],
            max_code=["E01033768", "W01001958", "S01013481", None],
        ),
        "msoa11": code_mask(
            mask="E########",
            min_code=["E02000001", "W02000001", "S02001236", None],
            max_code=["E02006934", "W02000423", "S02002514", None],
        ),
        "ru11ind": _(
            "choice",
            items=[
                code_mask(mask="&#", min_code="A1", max_code="F2"),
                code_mask(mask="#", min_code="1", max_code="8"),
                None,
            ],
        ),
        "oac11": _("choice", items=[code_mask(mask="#&#", min_code="1A1", max_code="8B3"), "9Z9", None]),
        "rgn": _(
            "choice",
            items=[
                code_mask(mask="E########", min_code="E12000001", max_code="E12000009"),
                "W92000004",
                "S92000003",
                None,
            ],
        ),
        "imd": _("choice", items=[_("random.randint", a=1, b=32844), None]),
        "interim_id": _("choice", items=[_("random.randint", a=1, b=138), None]),
    }

    schema = Schema(schema=sample_direct_eng_description)
    sample_direct_data = pd.DataFrame(schema.create(iterations=records))

    write_output(sample_direct_data, directory / f"sample_direct_eng_wc{file_date}.csv")


def generate_nims_table(table_name, participant_ids, records=10):
    spark_session = get_or_create_spark_session()
    schema = Schema(schema=get_nims_data_description(_, participant_ids))
    nims_pandas_df = pd.DataFrame(schema.create(iterations=records))
    nims_pandas_df["vaccination_date_dose_1"] = pd.to_datetime(
        nims_pandas_df["vaccination_date_dose_1"], format="%Y-%m-%d %H:%M:%S.%f"
    )
    nims_pandas_df["vaccination_date_dose_2"] = pd.to_datetime(
        nims_pandas_df["vaccination_date_dose_2"], format="%Y-%m-%d %H:%M:%S.%f"
    )
    nims_pandas_df["found_pds"] = pd.to_numeric(nims_pandas_df["found_pds"])
    nims_pandas_df["pds_conflict"] = pd.to_numeric(nims_pandas_df["pds_conflict"])
    nims_df = spark_session.createDataFrame(
        nims_pandas_df,
        schema="""cis_participant_id string, product_dose_1 string, vaccination_date_dose_1 timestamp,
        product_dose_2 string, vaccination_date_dose_2 timestamp, found_pds integer, pds_conflict integer""",
    )
    nims_df.write.saveAsTable(table_name, mode="overwrite")
    return nims_df


@register_pipeline_stage("generate_dummy_data")
def generate_dummy_data(output_directory):
    raw_dir = Path(output_directory) / "generated_data"
    swab_dir = raw_dir / "swab"
    blood_dir = raw_dir / "blood"
    survey_dir = raw_dir / "survey"
    northern_ireland_dir = raw_dir / "northern_ireland_sample"
    sample_direct_dir = raw_dir / "england_wales_sample"
    unprocessed_bloods_dir = raw_dir / "unprocessed_blood"
    historic_bloods_dir = raw_dir / "historic_blood"
    historic_swabs_dir = raw_dir / "historic_swab"
    historic_survey_dir = raw_dir / "historic_survey"
    for directory in [
        swab_dir,
        blood_dir,
        survey_dir,
        northern_ireland_dir,
        sample_direct_dir,
        unprocessed_bloods_dir,
        historic_bloods_dir,
        historic_swabs_dir,
        historic_survey_dir,
    ]:
        directory.mkdir(parents=True, exist_ok=True)

    file_datetime = datetime.now()
    lab_date_1 = datetime.strftime(file_datetime - timedelta(days=1), format="%Y%m%d")
    lab_date_2 = datetime.strftime(file_datetime - timedelta(days=2), format="%Y%m%d")
    file_date = datetime.strftime(file_datetime, format="%Y%m%d")

    # Historic files
    # historic_bloods = generate_historic_bloods_data(historic_bloods_dir, file_date, 30)
    # historic_swabs = generate_ons_gl_report_data(historic_swabs_dir, file_date, 30)

    # historic_v2 = generate_survey_v2_data(
    #     directory=historic_survey_dir,
    #     file_date=file_date,
    #     records=100,
    #     swab_barcodes=historic_swabs["Sample"].unique().tolist(),
    #     blood_barcodes=historic_bloods["blood_barcode_OX"].unique().tolist(),
    # )

    # Delta files
    lab_swabs_1 = generate_ons_gl_report_data(swab_dir, file_date, 10)
    lab_swabs_2 = generate_ons_gl_report_data(swab_dir, lab_date_1, 10)
    lab_swabs_3 = generate_ons_gl_report_data(swab_dir, lab_date_2, 10)
    lab_swabs = pd.concat([lab_swabs_1, lab_swabs_2, lab_swabs_3])

    lab_bloods_s_1, lab_bloods_n_1 = generate_unioxf_medtest_data(blood_dir, file_date, 10)
    lab_bloods_s_2, lab_bloods_n_2 = generate_unioxf_medtest_data(blood_dir, lab_date_1, 10)
    lab_bloods_s_3, lab_bloods_n_3 = generate_unioxf_medtest_data(blood_dir, lab_date_2, 10)

    lab_bloods = pd.concat(
        [lab_bloods_n_1, lab_bloods_n_2, lab_bloods_n_3, lab_bloods_s_1, lab_bloods_s_2, lab_bloods_s_3]
    )

    historic_blood_n = generate_historic_bloods_data(historic_bloods_dir, file_date, 10, "N")
    historic_blood_s = generate_historic_bloods_data(historic_bloods_dir, file_date, 10, "S")

    # unprocessed_bloods_data = generate_unprocessed_bloods_data(unprocessed_bloods_dir, file_date, 20)
    # northern_ireland_data = generate_northern_ireland_data(northern_ireland_dir, file_date, 20)
    # sample_direct_data = generate_sample_direct_data(sample_direct_dir, file_date, 20)

    # swab/blood barcode lists
    swab_barcode = lab_swabs["Sample"].unique().tolist()
    blood_barcode = lab_bloods["Serum Source ID"].unique().tolist()
    blood_barcode += historic_blood_n["blood_barcode_OX"].unique().tolist()
    blood_barcode += historic_blood_s["blood_barcode_OX"].unique().tolist()

    swab_barcode = swab_barcode[int(round(len(swab_barcode) / 10)) :]  # noqa: E203
    blood_barcode = blood_barcode[int(round(len(swab_barcode) / 10)) :]  # noqa: E203

    generate_survey_v0_data(
        directory=survey_dir, file_date=file_date, records=50, swab_barcodes=swab_barcode, blood_barcodes=blood_barcode
    )
    generate_survey_v1_data(
        directory=survey_dir, file_date=file_date, records=50, swab_barcodes=swab_barcode, blood_barcodes=blood_barcode
    )
    v2 = generate_survey_v2_data(
        directory=survey_dir, file_date=file_date, records=50, swab_barcodes=swab_barcode, blood_barcodes=blood_barcode
    )

    participant_ids = v2["Participant_id"].unique().tolist()

    generate_nims_table(get_full_table_name("cis_nims_20210101"), participant_ids)


if __name__ == "__main__":
    generate_dummy_data()
