swab_allowed_pcr_results = ["Inconclusive", "Negative", "Positive", "Rejected"]

swab_validation_schema = {
    "swab_sample_barcode": {"type": "string", "regex": r"ONS\d{8}"},
    "pcr_result_classification": {"type": "string", "allowed": ["Negative", "Positive", "Void"]},
    "pcr_datetime": {"type": "timestamp"},
    "pcr_lab_id": {"type": "string"},
    "pcr_method": {"type": "string"},
    "orf1ab_gene_pcr_target": {"type": "string", "allowed": ["ORF1ab"]},
    "orf1ab_gene_pcr_result_classification": {"type": "string", "allowed": swab_allowed_pcr_results},
    "orf1ab_gene_pcr_cq_value": {"type": "double", "nullable": True, "min": 0},
    "n_gene_pcr_target": {"type": "string", "allowed": ["N gene"]},
    "n_gene_pcr_result_classification": {"type": "string", "allowed": swab_allowed_pcr_results},
    "n_gene_pcr_cq_value": {"type": "double", "nullable": True, "min": 0},
    "s_gene_pcr_target": {"type": "string", "allowed": ["S gene"]},
    "s_gene_pcr_result_classification": {"type": "string", "allowed": swab_allowed_pcr_results},
    "s_gene_pcr_cq_value": {"type": "double", "nullable": True, "min": 0},
    "ms2_pcr_target": {"type": "string", "allowed": ["MS2"]},
    "ms2_pcr_result_classification": {"type": "string", "allowed": swab_allowed_pcr_results},
    "ms2_pcr_cq_value": {"type": "double", "nullable": True, "min": 0},
}


bloods_validation_schema = {
    #    "require_all": True,
    "blood_sample_barcode": {"type": "string", "regex": r"ONS\d{8}"},
    "blood_sample_type": {"type": "string", "allowed": ["Venous", "Capillary"]},
    "antibody_test_plate_id": {"type": "string", "regex": r"(ON[BS]|MIX)_[0-9]{6}[C|V]S(-[0-9]+)"},
    "antibody_test_well_id": {"type": "string", "regex": r"[A-Z][0-9]{2}"},
    "antibody_test_result_classification": {"type": "string", "allowed": ["DETECTED", "NOT detected", "failed"]},
    "antibody_test_result_value": {"type": "double", "nullable": True, "min": 0},
    "antibody_test_bounded_result_value": {"type": "string"},
    "antibody_test_undiluted_result_value": {"type": "string"},
    "antibody_test_result_recorded_date": {"type": "timestamp"},
    "blood_sample_arrayed_date": {"type": "timestamp"},
    "blood_sample_received_date": {"type": "timestamp"},
    "blood_sample_collected_datetime": {"type": "timestamp"},
}

{
    "Serum Source ID": "blood_sample_barcode",
    "Blood Sample Type": "blood_sample_type",
    "Plate Barcode": "antibody_test_plate_id",
    "Well ID": "antibody_test_well_id",
    "Detection": "antibody_test_result_classification",
    "Monoclonal Quantitation (Colourmetric)": "antibody_test_result_value",
    "Monoclonal Bounded Quantitation(Colourmetric)": "antibody_test_bounded_result_value",
    "Monoclonal undiluted Quantitation(Colourmetric)": "antibody_test_undiluted_result_value",
    "Date ELISA Result record created": "antibody_test_result_recorded_date",
    "Date Samples Arrayed Oxford": "blood_sample_arrayed_date",
    "Date Samples Received Oxford": "blood_sample_received_date",
    "Voyager Date Created": "blood_sample_collected_datetime",
}
