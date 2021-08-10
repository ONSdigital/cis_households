swab_variable_name_map = {
    "Sample": "swab_barcode",
    "Result": "swab_result",
    "Date Tested": "swab_pcr_test_date",
    "Lab ID": "swab_test_lab_id",
    "testKit": "test_kit",
    "CH1-Target": "orf1ab_gene_pcr_target",
    "CH1-Result": "orf1ab_gene_pcr_result",
    "CH1-Cq": "orf1ab_gene_pcr_cq_value",
    "CH2-Target": "n_gene_pcr_target",
    "CH2-Result": "n_gene_pcr_result",
    "CH2-Cq": "n_gene_pcr_cq_value",
    "CH3-Target": "s_gene_pcr_target",
    "CH3-Result": "s_gene_pcr_result",
    "CH3-Cq": "s_gene_pcr_cq_value",
    "CH4-Target": "ms2_pcr_target",
    "CH4-Result": "ms2_pcr_result",
    "CH4-Cq": "ms2_pcr_cq_value",
}


bloods_variable_name_map = {
    "Serum Source ID": "ons_id",
    "Blood Sample Type": "blood_sample_type",
    "Plate Barcode": "plate_tdi",
    "Well ID": "well_tdi",
    "Detection": "interpretation_tdi",
    "Monoclonal Quantitation (Colourmetric)": "tdi_assay_net_signal",
    "Monoclonal Bounded Quantitation(Colourmetric)": "monoclonal_bounded_quantitation",  # Drop this column
    "Monoclonal undiluted Quantitation(Colourmetric)": "monoclonal_undiluted_quantitation",  # Drop this column
    "Date ELISA Result record created": "run_date_tdi",
    "Date Samples Arrayed Oxford": "arrayed_dt",
    "Date Samples Received Oxford": "received_dt",
    "Voyager Date Created": "voyager_dt",
}
