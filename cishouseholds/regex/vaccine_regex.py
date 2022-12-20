pf_regex_match = "|".join(
    [
        r"P[FHJ]I[SZ][AE]R",
        r"PFZER",
        r"PFCIZER",
        r"PZIFER",
        r"CR?[AOU0]R?[MN]+[AEI]R?NR?[AI]R?I?TY",
        r"^BIOTONIC$",
        r"BIOTECH",
        r"BIOTECH MANUFACTURING",
        r"^BION?TECH?( GMBH)?$",
        r"BION?TECH MANUFACTURING",
        r"BION?TECH MANUFACTURING GMBH",
        r"BNT162?B2",
        r"COM.*TY",
        r"C[AO].*RTY",
        r"COR?MI.*Y",
        r"^IOTECH\?\.$",
        r"COM(IR|RI|I|OR)N[AO]T",
        r"COMIR?NA",
        r"CIMORATY",
        r"CO.{1,7}Y",
        r"TOZINAM[AE]R[AE]N.*RILTOZINAMERAN",
        r"^CR?OR?M.{0,4}$",
        r"COURAGEOUS",
        r"[OI]R?NARTY",
        r"DOMIRNATY",
        r"CEMETERY",
        r"COMISTAN",
        r"COMINETTI",
        r"COMUNIAL",
        r"CAMARATI",
        r"^FYSA$",
        r"^VIS[OE]R$",
        r"^CORMT?$",
        # "SARS[ |][-|]COV[-|]2[ |]MRNA[ |][VACCINE|]", #before 31stJan2021 only
        # "MR[M|N]A",#before 31stJan2021 only
        # "COVID[0-9] MRNA",#before 31stJan2021 only
        # "MRNACOVID",#before 31stJan2021 only
    ]
)

# before 31stJan2021 only
pf_date_dependent_regex_match = "|".join(
    [
        r"SARS[ |][-|]COV[-|]2[ |]MRNA[ |][VACCINE|]",
        r"MR[M|N]A",
        r"COVID[0-9] MRNA",
        r"MRNACOVID",
    ]
)


mod_regex_match = "|".join(
    [
        r"M[AO]DER?N(A|ER)",
        r"SP[AI]KE?V[AE][KX]",
        r"SPIKE",
        r"^SP(KI|OK)E$",
        r"SP.*V[AEOI][CKX]",
        r"^MOD.{0,3}$",
        r"SPI.{0,4}AX",
        r"S.{0,3}EVAX",
        r"PIKEVAX",
        r"^S.{0,2}VAX$",
        r"^MD$",
    ]
)
az_regex_match = "|".join(
    [
        r"(?<![A-Z])AZ(?![A-Z])",
        r"ASTR[AO]",
        r"COVID?SHIELD",
        r"COVIDHIELD",
        r"CHA(T\s*)?D\s*O\s*X",
        r"TRIUMPH",
        r"S[EC]RUM INSTITUTE OF INDIA",
    ]
)

jj_regex_match = "|".join(
    [
        "JAN+S+[AEO]+N",
        "JOHNSON",
    ]
)

novavax_regex_match = "|".join(
    [
        "NOV[AO][VX]A[CX]",
        "NO AVAVAX",
        "NOVAX",
        "NOVA VACCINE",
        "NOVAVA",
    ]
)

valneva_regex_match = "|".join(
    [
        "VA[L|R]+NE+[R|V]+A",
        "DALNEVA",
    ]
)

sputnik_regex_match = "|".join(
    [
        "SPUTNIK",
        "SPUTNIK V",
    ]
)

curevac_regex_match = "|".join(
    [
        "CUREVAC",
    ]
)

covaxin_regex_match = "|".join(
    [
        "COVAXIN",
    ]
)

covac1_regex_match = "|".join(
    [
        "COVAC1",
    ]
)

sanofi_regex_match = "|".join(
    [
        "SANOFI",
    ]
)

bivalent_regex_match = "|".join(
    [
        r"B[IA]+\s*V[AE]LE[CNTWU]T",
        r"OMN?[IO]N?CRO[MN]",
        r"OMR?ICI?ON",
        r"OMINICRON",
        r"OMACROM",
        r"AUTUMN",
        r"NEW BOOSTER",
        r"BI VALAR",
        r"BIVALLEN",
        r"BIALENT",
        r"BILAVENT",
        r"BIVA[CD]ENT",
        r"BIVALEN",
        r"DUAL VACCINE",
        r"DUAL BOOSTER",
    ]
)

trial_regex_match = "|".join(
    [
        r"TRIAL",
        r"BLIND (TRIAL|STUDY|TEST)",
        r"VACCINE TRIAL",
    ]
)

no_vacc_regex_match = "|".join(
    [
        r"INFLUEN[SZ]A",
        r"MEASLES",
        r"POLIO",
        r"MUMPS",
        r"RUBELLA",
        r"MMR",
        r"FLU",
        r"FLY BOOSTER",
        r"(NOT|UN)\s*VACCINATED",
        r"PNEUMONIA",
        r"PNEUMOCOCCAL",
        r"NO VAC+INE",
        r"SHINGLES",
        r"CHEMOTHERAPY",
        r"FORBIDDEN",
        r"H\s*P\s*V",
        r"QUADRIVALENT VACCINE",  # This is Flu A/B
        r"LATERAL\s*FLOW",
        r"^[^A-Z]$",
        r"SE[G|Q]IRUS",
    ]
)

unknown_regex_match = "|".join(
    [
        r"DO(ES)?\s*N.{0,4}T\s*K?NOW?",
        r"(NOT|UN)\s*K?NOWN",
        r"DO(ES)?\s*N.{0,4}T REMEMBER",
        r"NONE",
        r"NOT\s*SURE",
        r"[UI]N\s*SURE",
        r"UN\s*CERTAIN",
        r"NO NAME",
        r"NOT GIVEN (THE )?NAME",
        r"UNNAMED",
        r"UNKOWN",
        r"CAN\s*(NO)?.?T REMEMBER",
        r"COULD\s*N.?T (FIND|ANSWER)",
        r"(NOT|UN)\s*SPECIFIED",
        r"DOES\s*N.?T (SPECIFY|SAY)",
        r"^OTHER$",
        r"DID\s*N.?T ASK",
        r"WAS\s*N.?T GIVEN NAME",
        r"(WAS|HAVE?)\s*N.?T TOLD",
        r"WAS\s*N.?T INFORMED",
        r"NOT ON CARD",
        r"(NOT|UN)\s*READABLE",
        r"N/K",
        r"^DK",
    ]
)

vaccine_regex_map = {
    "Don't know type": unknown_regex_match,
    "From a research study/trial": trial_regex_match,
    "Moderna": mod_regex_match,
    "Oxford/AstraZeneca": az_regex_match,
    "Pfizer/BioNTech": pf_regex_match,
    "Pfizer/BioNTechDD": pf_date_dependent_regex_match,
    "Janssen/Johnson&Johnson": jj_regex_match,
    "Novavax": novavax_regex_match,
    "Sinovac": "(SINOVAC|SIROFAX)",
    "Sinovax": "SINOVAX",
    "Valneva": valneva_regex_match,
    "Sinopharm": "SINOPH",
    "Sputnik": sputnik_regex_match,
    "Bivalent": bivalent_regex_match,
    "": no_vacc_regex_match,
    "Covac": covac1_regex_match,
    "Covaxin": covaxin_regex_match,
    "Curevac": curevac_regex_match,
    "Sanofi": sanofi_regex_match,
}

vaccine_regex_priority_map = {
    "Pfizer/BioNTechDD": 1,
    "Pfizer/BioNTech": 1,
    "Moderna": 3,
    "Oxford/AstraZeneca": 4,
    "Don't know type": 5,
    "Bivalent": 6,
    "Novavax": 7,
    "Janssen/Johnson&Johnson": 8,
    "Sinopharm": 9,
    "Sputnik": 10,
    "Sinovax": 11,
    "Valneva": 12,
    "": 13,
    "Covac": 14,
    "Covaxin": 15,
    "Curevac": 16,
    "Sanofi": 17,
    "Sinovac": 18,
}
