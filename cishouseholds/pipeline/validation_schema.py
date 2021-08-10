swab_allowed_pcr_results = ["Inconclusive", "Negative", "Positive", "Rejected"]

swab_validation_schema = {
    "require_all": True,
    "swab_barcode": {"type": "string", "regex": r"ONS\d{8}"},
    "swab_result": {"type": "string", "allowed": ["Negative", "Positive", "Void"]},
    "swab_pcr_test_date": {"type": "timestamp"},
    "swab_test_lab_id": {"type": "string"},
    # test_kit is dropped in initial editing
    "orf1ab_gene_pcr_target": {"type": "string", "allowed": ["ORF1ab"]},
    "orf1ab_gene_pcr_result": {"type": "string", "allowed": swab_allowed_pcr_results},
    "orf1ab_gene_pcr_cq_value": {"type": "double", "nullable": True, "min": 0},
    "n_gene_pcr_target": {"type": "string", "allowed": ["N gene"]},
    "n_gene_pcr_result": {"type": "string", "allowed": swab_allowed_pcr_results},
    "n_gene_pcr_cq_value": {"type": "double", "nullable": True, "min": 0},
    "s_gene_pcr_target": {"type": "string", "allowed": ["S gene"]},
    "s_gene_pcr_result": {"type": "string", "allowed": swab_allowed_pcr_results},
    "s_gene_pcr_cq_value": {"type": "double", "nullable": True, "min": 0},
    "ms2_pcr_target": {"type": "string", "allowed": ["MS2"]},
    "ms2_pcr_result": {"type": "string", "allowed": swab_allowed_pcr_results},
    "ms2_pcr_cq_value": {"type": "double", "nullable": True, "min": 0},
}


bloods_validation_schema = {
    "require_all": True,
    "ons_id": {"type": "string", "regex": r"ONS\d{8}"},
    "blood_sample_type": {"type": "string", "allowed": ["Venous", "Capillary"]},
    "plate_tdi": {"type": "string", "regex": r"(ON[BS]|MIX)_[0-9]{6}[C|V]S(-[0-9]+)"},
    "well_tdi": {"type": "string", "regex": r"[A-Z][0-9]{2}"},
    "interpretation_tdi": {"type": "string", "allowed": ["DETECTED", "NOT detected", "failed"]},
    "tdi_assay_net_signal": {"type": "float", "nullable": True, "min": 0},  # Should this be a string?
    #   "monoclonal_bounded_quantitation": {"type": "string"},  # Drop this column
    #   "monoclonal_undiluted_quantitation": {"type": "string"},  # Drop this column
    "run_date_tdi": {"type": "timestamp"},
    "arrayed_dt": {"type": "timestamp"},
    "received_dt": {"type": "timestamp"},
    "voyager_dt": {"type": "timestamp"},
}
