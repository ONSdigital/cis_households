from collections import namedtuple

RegexPattern = namedtuple("RegexPattern", ["positive_regex_pattern", "negative_regex_pattern"])


def match_with_exclusions(patters_to_match, patterns_to_exclude=[]):
    if type(patters_to_match) != list:
        patters_to_match = [patters_to_match]
    if type(patterns_to_exclude) != list:
        patterns_to_exclude = [patterns_to_exclude]
    patterns_to_exclude = "|".join(patterns_to_exclude)
    regex = ""
    for pattern in patters_to_match:
        regex += rf"(?=.*?({pattern}))"
    return regex + rf"(^(?!.*({patterns_to_exclude})).*)"


# exclusions
teaching_exclusions = match_with_exclusions(
    r"EDUCATION|SCHOOL|BUSINESS|STUDYING|LECTURER|PROFESSOR|ACADEMIC|RESEARCH|UNIVERSITY|TEACH|STUDENT|TECHNICIAN|DOCTORATE|PHD|POSTDOCTORAL|AC[AE]DEMIC|RESEAR*CH|LAB(ORATORY)?|DATA|ANAL|STATIST|EPIDEMI|EXAM|EDUCAT|EARLY YEARS|COLL.GE|TEACH|LECTURE|PROFESS|HOUSE *(M[AI]ST(ER|RESS)|PARENT)|COACH|TRAIN|INSTRUCT|TUTOR|LEARN",
    "MEDICAL",
)
catering_exclusions = r"CHEF|SOUS|COOK|CATER|BREWERY|CHEESE|KITCHEN|KFC|CULINARY|FARM(ER|ING)"
media_exclusions = r"BROADCAST|JOURNALIST|CAMERA|WRIT|COMMUNICAT|CURAT(OR)*|MARKETING|MUSICIAN|ACT([OE]R|RESS)|ARTIST"
retail_exclusions = r"RETAIL|BUYER|SALE|BUY AND SELL|CUSTOMER|AGENT|BANK(ING|ER)|INSURANCE|BEAUT(Y|ICIAN)?|NAIL|HAIR|SHOP|PROPERTY|TRADE|SUPER *MARKET|WH *SMITH|TESCO"
domestic_exclusions = r"DOMESTIC|CLEAN|LAU*ND.*Y"
construction_exclusions = r"BUILD|CONSTRUCT|RENOVAT|REFIT|ENGINE|PLANT|CR[AI]*NE*|SURVEY(OR)*|DESIGNER|ARCHITECT|TECHNICIAN|MECHAN|MANUFACT|ELECTRIC|CARPENTER|PLUMB|WELD(ER|ING)|PASTER(ER|ING)|\bEE\b|GARDE*N|FURNITURE|MAINT[AIE]*N*[EA]N*CE|\bGAS\b|JOINER"
religious_exclusions = r"CHAPL[AI]*N|VICAR|CLERGY|MINISTER|PREACH|CHURCH"
it_exclusions = r"\bI[ \.]*T\.?\b|DIGIT|WEBSITE|NETWORK|DEVELOPER|SOFTWARE|SYSTEM"
public_service_exclusions = r"CHAIR|CHARITY|CITIZEN|CIVIL|VOLUNT|LIBRAR|TRANSLAT|INVESTIGAT|FIRE ?(WO)?(M[AE]N|FIGHT)|POLICE|POST *(WO)*MAN|PRISON|FIRST AID|SAFETY|\bTAX\b|LI[CS][EA]N[CS]E|LEA*GAL|LAWYER|\bLAW\b|SO*LICITOR"
transport_exclusions = r"DRIV(E|ER|ING)|PILOT|TRAIN DRIVE|TAXI|LORRY|TRANSPORT|DELIVER|SUPPLY"
base_exclusions = r"AC[AE]DEMIC|LECTURE|DEAN|DOCTOR SCIENCE|DR LAB|DATA ANAL|AC?OUNT(ANT|ANCY)?|WARE *HOUSE|TRADE UNION|SALES (MANAGER|REP)|INVESTIGATION OF+ICE|AC+OUNT|PRISI?ON|DIRECT[OE]R"


# base inclusions
base_inclusions = r"(PALLIATIVE|INTENSIVE) CARE|TRIAGE|CHIROPRACT"

vet = match_with_exclusions(r"\bVETS*|\bVEN?T[A-Z]*(RY|IAN)|EQUIN|\b(DOG|CAT)|HEDGEHOG|ANIMAL", "VET PEOPLE")
hc_admin = match_with_exclusions(
    [
        r"\bADMIN(?!IST[EO]R)|ADM[A-Z]{2,}RAT[EO]R|CLERICAL|CLERK",
        r"NHS|HOSPITAL|MEDICAL|SURG[EA]RY|CLINIC|HEALTH *CARE|CLINICAL *CODER|\bWARD *CLERK",
    ],
    r"STATIST|SCHOOL|LOCAL *GOVERNMENT|CIVIL *SERV(ANT|ICE)|^BANK CLERK|\bCHURCH\b",
)
hc_counsellor = match_with_exclusions(
    [r"COUN(S|C)", r"ADDICT|VICTIM|TRAUMA|\sMENTAL HEALTH|DRUG|ALCOHOL|ABUSE|SUBSTANCE"],
    [
        r"REPRESENT|BUSINESS|POLI(C|T)|CAREER|DISTRICT|LOCAL|COUNTY|DEBT|CITY|COUNCIL\s|COUNCIL$|ACCOUNTANT|SOLICITOR|LAW|CHAPLAN|CHAPLAIN|DEFENCE|GOVERNMENT|PARISH|LAWYER|ASSESSOR|CURRICULUM|LEGAL|PRISONER|FARMER|CASEWORK|CARE WORK",
        teaching_exclusions,
    ],
)
hc_receptionist = match_with_exclusions(
    [
        r"RECEPTIONIST|OPTICAL ASSISTANT|RECEPTION *(WORK|DUTIES)",
        r"NHS|HOSPITAL$|OSTEOPATH|OUTPATIENT|HOSPITAL(?!ITY)|MEDICAL|SURG[EA]RY|CLINIC|HEALTH *CARE|DENTAL|DENTIST|\bGP\b|\bDOCTOR|OPTICIAN|OPTICAL|CHIROPRAC|A&E",
    ],
    [
        r"SCHOOL|LOCAL *GOVERNMENT|CIVIL *SERV(ANT|ICE)|\bCHURCH\b|\bHOTEL|\bCARE *HOME|\bVET[A-Z]*RY\b|HAIR *(SALON|DRESS)+|EDUCATION|SPORT[S ]*CENT|LEISURE|BEAUTY|COLLEGE",
        r"\bSPA\b|RETAIL|\bLAW\b\|\bLEGAL|\bBAR WORK|GARAGE|\bVET[S]*\b",
        r"LOCAL *GOVERNMENT|CIVIL *SERV(ANT|ICE)|\bCHURCH\b|\bHOTEL",
    ],
)
hc_secretary = match_with_exclusions(
    [
        r"S.?C+R+.?T+.?R+Y|\sPA\s|P.?RS+.?N+.?L AS+IS+T+AN+",
        r"MEDIC.*|HEALTH|NHS|HOSPITAL\s|HOSPITAL$|CLINIC PATIENT|CAMHS|X.?RAY|\sDR\s|DOCTOR|PAEDIATRIC|A&E|DENTIST|PATIENT|GP|DENTAL|SURGERY|OPTI.*|OPTICAL|OPTICIANS",
    ],
    [
        r"LEGAL|LAW|SOLICITOR|PRODUCTION|PARISH|DOG|INTERNATIONAL|COMPANY|COMPANY|EDUCATION|UNIVERSITY|SCHOOL|TEACHER|FINANCE|BUILDER|BUSINESS|BANK|PROJECT|CHURCH|ESTATE AGENT|MANUFACT|SALE|SPORT|FARM|CLUB",
        r"CONTRACTOR|CIVIL SERV.*|CLERICAL|COUNCIL|MEDICAL SCHOOL|ACCOUNT|CARER|CHARITY",
    ],
)
hc_support = match_with_exclusions(
    [
        r"SUP+ORT *WORKER",
        r"HOSPITAL|HEALTH *CARE|MENTAL *HEALTH|MATERNITY|CLINICAL|WARD|NURSE|NURSING(?! *HOME)|SURGERY|A&E|ONCOLOGY|PHLEBOTOM|AMBULANCE|WARD|MIDWIFE|ACCIDENT *(& *|AND *)*EMERGENCY|COVID.*SWA[BP]",
    ],
    [r"BU(IS|SI)NESS SUPPORT", r"LOCAL COUNCIL|DISCHARGE|POST HOSPITAL|HOME NURS", r"HEALTH *CARE ASSIST|\bHCA\b"],
)
domestic = match_with_exclusions(
    r"(HOME|HOUSE|DOMESTIC) *CARE|CARER* OF HOME|HOUSE *WIFE|HOME *MAKER",
    r"(?<!MENTAL )HEALTH *CARE|CRITICAL CARE|(?<!NO[NT][ -])MEDICAL|DONOR CARER*|HOSPITAL",
)
child_care = match_with_exclusions(
    r"CHILD *(CARE|MIND)|NANN[YIE]+\b|AU PAIR", "(?<!MENTAL )HEALTH *CARE|CRITICAL CARE|MEDICAL|DONOR CARER*|HOSPITAL"
)
informal_care = match_with_exclusions(
    r"(((CAR(ER|ING)+|NURSE) (FOR|OF))|LOOKS* *AFTER) *(MUM|MOTHER|DAD|FATHER|SON|D[AU]+GHT|WIFE|HUSB|PARTNER|CHILD|FAM|T*H*E* ELDERLY)",
    r"(?<!MENTAL )HEALTH *CARE|CRITICAL CARE|MEDICAL|DONOR CARER*|HOSPITAL",
)
house_care = match_with_exclusions(
    r"(HOME|HOUSE|DOMESTIC) *CARE|CARER* OF HOME|HOUSE *WIFE|HOME *MAKER",
    r"(?<!MENTAL )HEALTH *CARE|CRITICAL CARE|(?<!NO[NT][ -])MEDICAL|DONOR CARER*|HOSPITAL",
)
formal_care = match_with_exclusions(
    r"^CAE?RE*R *(CARE*|NA)*$|(CARE|NURSING) *HOME|(SOCIAL|COMMUNITY|DOMICIL[IA]*RY)* *CARE|CARE *(WORK|ASSISTANT)|ASST CARING|CARE SUPPORT WORK|SUPPORT *WORKER *CARE|INDEPEND[EA]NT LIVING",
    r"(?<!MENTAL )HEALTH *CARE|CRITICAL CARE|MEDICAL|DONOR CARER*|HOSPITAL",
)
pharmacist = match_with_exclusions(
    [r"PHARMA(?![CS][EU]*TIC)", r"AS+IST|TECHN|RETAIL|DISPEN[SC]|SALES AS+IST|HOSPITAL|PRIMARY CARE|SERVE CUSTOM"],
    r"ANALYST|CARE HOME|ELECTRICAL|COMPAN(Y|IES)|INDUSTR|DIRECTOR|RESEARCH|WAREHOUSE|LAB|PROJECT|PRODUCTION|PROCESS|LAB|QA|QUALITY",
)
dietician = match_with_exclusions(r"\bD[EIA]{0,2}[TC][EI]?[CT]+[AEIOU]*[NC(RY)]|\bDIET(RIST)?\b", "DETECTION")
doctor = match_with_exclusions(
    r"DOCT[EO]R|\bGP\b|GENERAL PRACTI(CIAN|TION)|\bDR\b|CARDIAC|\A ?(&|AND) ?E\|PHYSI[CT]I[AO]",
    r"LECTURER|DOCTORI*AL|RESEARCH|PHD|STAT|WAR|ANIMAL|SALES*|FINANCE",
)
general_practitioner = r"GP|GENERAL PRACTIT.*ER"
dentist = r"DENTIS.*|\bDENTAL|ORAL HEALTH"
midwife = r"MI*D*.?WI*F.?E.?|MIDWIV|MID*WIF|HEALTH VISITOR"
nurse = match_with_exclusions(
    r"N[IU]RS[EY]|MATRON|\bHCA\b", r"N[UI][RS]S[EA]R*[YIEU]+|(CARE|NURSING) *HOME|SCHOOL|TEACHER"
)
paramedic = match_with_exclusions("PARA *MEDIC|AMBUL[AE]NCE", teaching_exclusions)
covid_test = match_with_exclusions(["COVID", "TEST|SWAB|VAC+INAT|IM+UNIS|SCREEN|WARD"], "LAB|AN[AY]LIST|SCHOOL|MANAGER")
physiotherapist = match_with_exclusions(
    r"PH[YI]+SIO|PH[YSIH]+IO\s*THERAPIST|PH[YI]S[IY]CAL\s*REHAB|PH[YI]S[IY]CAL\s*THERAPY",
    match_with_exclusions(
        [
            r"PHSYCOLOGY|PHYCOLOGIST|PYCHLOLOGIST|PHYCOLOGIST|PSYCHOLOGOLOGIST|PHYCOLIGIST|PYSCOLOGIST|PHYSCHOLOGICAL|PSYCHOLOGICIST|PSYCHOLGIST|PSYCHOLOGIST|PSYCHOLOGY|PSYCHOLOGICAL",
            r"EDUCATION|SCHOOL|BUSINESS|STUDYING|LECTURER|PROFESSOR|ACADEMIC|RESEARCH|UNIVERSITY|TEACHING|TEACH|STUDENT|TECHNICIAN|DOCTORATE|PHD|POSTDOCTORAL|PRISON|OCCUPATION|OCCUPATIONAL|FORENSICS|\bUCL\b",
        ]
    ),
)
hc_theatre = match_with_exclusions("THEATRE", "PERFORMER|ACTOR|STAGE|PLAY")
social_work = "SOCIAL.*WORK|FOSTER CARE"
support_work = "SUP+ORT *WORKER"
apprentice = "AP*RENTI[CS]"
call_operator = match_with_exclusions(
    [
        r"111|119|999|911|NHS|TRIAGE|EMERGENCY",
        r"ADVI[SC][OE]R|RESPONSE|OPERAT|CALL (HANDLER|CENT(RE|ER)|TAKE)|(TELE)?PHONE|TELE(PHONE)?|COVID",
    ],
    "CUSTOMER SERVICE|SALES",
)


additional_hc = match_with_exclusions(
    r"SONOGRAPHER|RADIO(GRAPHER|LOGIST|LOGY)|VAC+INAT[OE]R|SURGE(ON|ERY)|(DIABETIC EYE|RETINAL) SCRE+NER|(PH|F)LEBOTOM|CLINICAL SCIEN|"
    + r"MEDICAL PHYSICIST|CARDIAC PHYSIOLOG|OSTEOPATH|OPTOMOTRIST|PODIATRIST|OBSTETRI|GYNACOLOG|ORTHO(DOENT)?+|OPTI[TC]I[AO]N|CRITICAL CARE PRACTITIONER|HOSPITAL PORTER|AN[AE]STHET(IST|IC|IA)|"
    + r"PALLIATIVE|DISTRICT NURS|PAEDIATRI[CT]I[AO]N|HAEMATOLOGIST",
    r"LAB MANAGER",
)
additional_primary_hc = r"OUTPATIENT"
additional_secondary_hc = r"ANA?ETH|SURGE(ON|ERY)|\bOPD\b|\bITU\b|(SPEECH|LANGUAGE) THERAPIST|MICROB.*IST|RADIO(GRAPHER|LOGIST|LOGY)|PHYSCOL.*IST|PAEDIATRI[CT]I[AO]N"
other_hc = r"OPTICIAN|OPTHAL.*IST|ACCUP.*RE|THERAPIST|FOOT PRACTI(CIAN|TION)|HOMEOPATHY|PHYSCOT.*IST|\bMORT|HOSPITAL|\bNHS\b|MEDICAL EQUIP|TEACHING MEDICAL|WELLBEING PRACTITIONER"

# patient facing
patient_facing_negative_regex = match_with_exclusions(
    r"ONLINE|ZOOM|MICROSOFT|MS TEAMS|SKYPE|GOOGLE HANGOUTS?|REMOTE|VIRTUAL|(ONLY|OVER THE) (TELE)?PHONE|((TELE)?PHONE|VIDEO) (CONSULT|CALL|WORK|SUPPORT)|(NO[TN]( CURRENTLY)?|NEVER) (IN PERSON|FACE TO FACE)|SH[EI]+LDING|WORK(ING)? (FROM|AT) HOME|HOME ?BASED|DELIVER(Y|ING)? PRESCRI",
    r"(?<!NOT )OFFICE BASED",
)
patient_facing_positive_regex = "|".join(
    [
        r"PALLIATIVE CARE|(?<!NOT )PATI[EA]NT FACING|(LOOK(S|ING)? AFTER|SEES?|CAR(E|ING) (OF|FOR)) PATI[EA]NTS|(?<!NO )FACE TO FACE|(?<!NOT )FACE TO FACE",
        r"(?<!NO )(DIRECT )?CONTACT WITH PATI[EA]NTS|CLIENTS COME TO (HER|HIS|THEIR) HOUSE",
    ]
)

primary_healthcare_roles = ["nurse", "doctor", "general_practitioner"]

healthcare_negative_roles = [
    "transport",
    "catering",
    "teaching",
    "media",
    "retail",
    "domestic",
    "construction",
    "religion",
    "IT",
    "public_service",
    "vet",
    "house_care",
    "child_care",
    "informal_care",
    "formal_care",
    "social_work",
    "base_non_healthcare",
]

healthcare_positive_roles = [
    "hc_admin",
    "hc_secretary",
    "hc_receptionist",
    "hc_counsellor",
    "hc_support",
    "pharmacist",
    "call_operator",
    "patient_facing_positive_regex",
    "dietician",
    "doctor",
    "dentist",
    "midwife",
    "nurse",
    "paramedic",
    "physiotherapist",
    "covid_tester",
    "base_healthcare",
    "hc_theatre",
]

non_patient_facing_roles = ["hc_receptionist", "hc_counsellor", "hc_support"]

roles_map = {
    "hc_admin": hc_admin,
    "hc_theatre": hc_theatre,
    "hc_secretary": hc_secretary,
    "hc_receptionist": hc_receptionist,
    "hc_counsellor": hc_counsellor,
    "hc_support": hc_support,
    "pharmacist": pharmacist,
    "call_operator": call_operator,
    "patient_facing_positive_regex": patient_facing_positive_regex,
    "dietician": dietician,
    "doctor": doctor,
    "general_practitioner": general_practitioner,
    "dentist": dentist,
    "midwife": midwife,
    "nurse": nurse,
    "paramedic": paramedic,
    "physiotherapist": physiotherapist,
    "covid_tester": covid_test,
    "base_healthcare": base_inclusions,
    "base_non_healthcare": base_exclusions,
    "transport": transport_exclusions,
    "catering": catering_exclusions,
    "teaching": teaching_exclusions,
    "media": media_exclusions,
    "retail": retail_exclusions,
    "domestic": domestic_exclusions,
    "construction": construction_exclusions,
    "religion": religious_exclusions,
    "IT": it_exclusions,
    "public_service": public_service_exclusions,
    "vet": vet,
    "house_care": house_care,
    "child_care": child_care,
    "informal_care": informal_care,
    "formal_care": formal_care,
    "social_work": social_work,
    "additional_healthcare": additional_hc,
    "apprentice": apprentice,
}

patient_facing_pattern = RegexPattern(
    positive_regex_pattern=patient_facing_positive_regex, negative_regex_pattern=patient_facing_negative_regex
)
