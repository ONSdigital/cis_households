swab_allowed_pcr_results = ["Inconclusive", "Negative", "Positive", "Rejected"]

swab_validation_schema = {
    "swab_sample_barcode": {"type": "string", "regex": r"ONS\d{8}"},
    "pcr_test_result_classification": {"type": "string", "allowed": ["Negative", "Positive", "Void"]},
    "pcr_test_datetime": {"type": "timestamp"},
    "pcr_test_lab_id": {"type": "string"},
    "pcr_test_method": {"type": "string"},
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
