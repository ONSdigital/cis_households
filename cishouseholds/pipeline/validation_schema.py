swabs_allowed_pcr_results = ["Inconclusive", "Negative", "Positive", "Rejected"]

swabs_validation_schema = {
    "require_all": True,
    "swab_barcode": {"type": "string", "regex": r"ONS\d{8}"},
    "swab_result": {"type": "string", "allowed": ["Negative", "Positive", "Void"]},
    "swab_pcr_test_date": {"type": "timestamp"},
    "swab_test_lab_id": {"type": "string"},
    # test_kit is dropped in initial editing
    "orf1ab_gene_pcr_target": {"type": "string", "allowed": ["ORF1ab"]},
    "orf1ab_gene_pcr_result": {"type": "string", "allowed": swabs_allowed_pcr_results},
    "orf1ab_gene_pcr_cq_value": {"type": "double", "nullable": True, "min": 0},
    "n_gene_pcr_target": {"type": "string", "allowed": ["N gene"]},
    "n_gene_pcr_result": {"type": "string", "allowed": swabs_allowed_pcr_results},
    "n_gene_pcr_cq_value": {"type": "double", "nullable": True, "min": 0},
    "s_gene_pcr_target": {"type": "string", "allowed": ["S gene"]},
    "s_gene_pcr_result": {"type": "string", "allowed": swabs_allowed_pcr_results},
    "s_gene_pcr_cq_value": {"type": "double", "nullable": True, "min": 0},
    "ms2_pcr_target": {"type": "string", "allowed": ["MS2"]},
    "ms2_pcr_result": {"type": "string", "allowed": swabs_allowed_pcr_results},
    "ms2_pcr_cq_value": {"type": "double", "nullable": True, "min": 0},
}
