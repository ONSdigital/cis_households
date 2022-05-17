# import pytest
# from cishouseholds.weights.pre_calibration import DesignWeightError
# from cishouseholds.weights.weights import validate_design_weights_or_precal
# def test_validate_design_weights(spark_session):
#     input1_df = spark_session.createDataFrame(  # fails check 1 due to erroneous antibody weight
#         data=[
#             (1, 1.0, 2.0, 1.0, 1.0, 6.0),
#             (1, 1.0, 2.0, 1.0, 1.0, 6.0),
#             (1, 1.0, 2.0, 1.0, 1.0, 6.0),
#             (1, 1.0, 1.0, 1.0, 1.0, 6.0),
#             (1, 1.0, 1.0, 111.0, 1.0, 6.0),
#             (1, 1.0, 1.0, 1.0, 1.0, 6.0),
#         ],
#         schema="""
#             window integer,
#             weight1 double,
#             weight2 double,
#             swab_weight double,
#             antibody_weight double,
#             num_hh double
#             """,
#     )
#     input2_df = spark_session.createDataFrame(  # fails check 1 due to erroneous antibody weight
#         data=[
#             (1, 1.0, 1.0, 1.0, 1.0, 6.0),
#             (1, 1.0, 1.0, 1.0, 1.0, 6.0),
#             (1, 1.0, 1.0, 1.0, 1.0, 6.0),
#             (1, 1.0, 1.0, 1.0, 1.0, 6.0),
#             (1, 1.0, 1.0, 1.0, 1.0, 6.0),
#             (1, 1.0, 1.0, 1.0, 1.0, 6.0),
#         ],
#         schema="""
#             window integer,
#             weight1 double,
#             weight2 double,
#             swab_weight double,
#             antibody_weight double,
#             num_hh double
#             """,
#     )
#     with pytest.raises(DesignWeightError):
#         validate_design_weights_or_precal(
#             df=input1_df,
#             num_households_by_cis_column="num_hh",
#             num_households_by_country_column="num_hh",
#             swab_weight_column="swab_weight",
#             antibody_weight_column="antibody_weight",
#             group_by_columns=["window"],
#         )
#     validate_design_weights_or_precal(
#         df=input2_df,
#         num_households_by_cis_column="num_hh",
#         num_households_by_country_column="num_hh",
#         swab_weight_column="swab_weight",
#         antibody_weight_column="antibody_weight",
#         group_by_columns=["window"],
#     )
