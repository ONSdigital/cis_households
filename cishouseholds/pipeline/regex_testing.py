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
teaching_exclusions = "EDUCATION|SCHOOL|BUSINESS|STUDYING|LECTURER|PROFESSOR|ACADEMIC|RESEARCH|UNIVERSITY|TEACH|STUDENT|TECHNICIAN|DOCTORATE|PHD|POSTDOCTORAL|AC[AE]DEMIC|RESEAR*CH|SCIEN|LAB(ORATORY)?|DATA|ANAL|STATIST|EPIDEMI|EXAM|EDUCAT|EARLY YEARS|COLL.GE|TEACH|LECTURE|PROFESS|HOUSE *(M[AI]ST(ER|RESS)|PARENT)|COACH|TRAIN|INSTRUCT|TUTOR|LEARN"
catering_exclusions = "CHEF|SOUS|COOK|CATER|BREWERY|CHEESE|KITCHEN|KFC|CULINARY|FARM(ER|ING)"
media_exclusions = "BROADCAST|JOURNALIST|CAMERA|WRIT|COMMUNICAT|CURAT(OR)*|MARKETING|MUSICIAN|ACT([OE]R|RESS)|ARTIST"
retail_exclusions = "RETAIL|BUYER|SALE|BUY AND SELL|CUSTOMER|AGENT|BANK(ING|ER)|INSURANCE|BEAUT(Y|ICIAN)?|NAIL|HAIR|SHOP|PROPERTY|TRADE|SUPER *MARKET|WH *SMITH|TESCO"
domestic_exclusions = "DOMESTIC|CLEAN|LAU*ND.*Y"
construction_exclusions = "BUILD|CONSTRUCT|RENOVAT|REFIT|ENGINE|PLANT|CR[AI]*NE*|SURVEY(OR)*|DESIGNER|ARCHITECT|TECHNICIAN|MECHAN|MANUFACT|ELECTRIC|CARPENTER|PLUMB|WELD(ER|ING)|PASTER(ER|ING)|\\bEE\\b|GARDE*N|FURNITURE|MAINT[AIE]*N*[EA]N*CE|\\bGAS\\b|JOINER"
religious_exclusions = "CHAPL[AI]*N|VICAR|CLERGY|MINISTER|PREACH|CHURCH"
it_exclusions = "\\bI[ \\.]*T\\.?\\b|DIGIT|WEBSITE|NETWORK|DEVELOPER|SOFTWARE|SYSTEM"
public_service_exclusions = "CHAIR|CHARITY|CITIZEN|CIVIL|VOLUNT|LIBRAR|TRANSLAT|INVESTIGAT|FIRE ?(WO)?(M[AE]N|FIGHT)|POLICE|POST *(WO)*MAN|PRISON|FIRST AID|SAFETY|\\bTAX\\b|LI[CS][EA]N[CS]E|LEA*GAL|LAWYER|\\bLAW\\b|SO*LICITOR"
transport_exclusions = "DRIV(E|ER|ING)|PILOT|TRAIN DRIVE|TAXI|LORRY|TRANSPORT|DELIVER|SUPPLY"
base_exclusions = "AC[AE]DEMIC|LECTURE|DEAN|DOCTOR SCIENCE|DR LAB|DATA ANAL|AC?OUNT(ANT|ANCY)?|WARE *HOUSE|TRADE UNION|SALES (MANAGER|REP)|INVESTIGATION OF+ICE|AC+OUNT|PRISI?ON|DIRECT[OE]R"


# base inclusions
base_inclusions = "(PALLIATIVE|INTENSIVE) CARE|TRIAGE|CHIROPRACT"

vet = match_with_exclusions("\\bVETS*|\\bVEN?T[A-Z]*(RY|IAN)|EQUIN|\\b(DOG|CAT)|HEDGEHOG|ANIMAL", "VET PEOPLE")
hc_admin = match_with_exclusions(
    [
        "\\bADMIN(?!IST[EO]R)|ADM[A-Z]{2,}RAT[EO]R|CLERICAL|CLERK",
        "NHS|HOSPITAL|MEDICAL|SURG[EA]RY|CLINIC|HEALTH *CARE|CLINICAL *CODER|\\bWARD *CLERK",
    ],
    "STATIST|SCHOOL|LOCAL *GOVERNMENT|CIVIL *SERV(ANT|ICE)|^BANK CLERK|\\bCHURCH\\b",
)
hc_counsellor = match_with_exclusions(
    ["COUN(S|C)", "ADDICT|VICTIM|TRAUMA|\\sMENTAL HEALTH|DRUG|ALCOHOL|ABUSE|SUBSTANCE"],
    [
        "REPRESENT|BUSINESS|POLI(C|T)|CAREER|DISTRICT|LOCAL|COUNTY|DEBT|CITY|COUNCIL\\s|COUNCIL$|ACCOUNTANT|SOLICITOR|LAW|CHAPLAN|CHAPLAIN|DEFENCE|GOVERNMENT|PARISH|LAWYER|ASSESSOR|CURRICULUM|LEGAL|PRISONER|FARMER|CASEWORK|CARE WORK",
        teaching_exclusions,
    ],
)
hc_receptionist = match_with_exclusions(
    [
        "RECEPTIONIST|OPTICAL ASSISTANT|RECEPTION *(WORK|DUTIES)",
        "NHS|HOSPITAL$|OSTEOPATH|OUTPATIENT|HOSPITAL(?!ITY)|MEDICAL|SURG[EA]RY|CLINIC|HEALTH *CARE|DENTAL|DENTIST|\\bGP\\b|\\bDOCTOR|OPTICIAN|OPTICAL|CHIROPRAC|A&E",
    ],
    [
        "SCHOOL|LOCAL *GOVERNMENT|CIVIL *SERV(ANT|ICE)|\\bCHURCH\\b|\\bHOTEL|\\bCARE *HOME|\\bVET[A-Z]*RY\\b|HAIR *(SALON|DRESS)+|EDUCATION|SPORT[S ]*CENT|LEISURE|BEAUTY|COLLEGE",
        "\\bSPA\\b|RETAIL|\\b\\LAW\\b\\|\\bLEGAL|\\bBAR WORK|GARAGE|\\bVET[S]*\\b",
        "LOCAL *GOVERNMENT|CIVIL *SERV(ANT|ICE)|\\bCHURCH\\b|\\bHOTEL",
    ],
)
hc_secretary = match_with_exclusions(
    [
        "S.?C+R+.?T+.?R+Y|\\sPA\\s|P.?RS+.?N+.?L AS+IS+T+AN+",
        "MEDIC.*|HEALTH|NHS|HOSPITAL\\s|HOSPITAL$|CLINIC PATIENT|CAMHS|X.?RAY|\\sDR\\s|DOCTOR|PAEDIATRIC|A&E|DENTIST|PATIENT|GP|DENTAL|SURGERY|OPTI.*|OPTICAL|OPTICIANS",
    ],
    [
        "LEGAL|LAW|SOLICITOR|PRODUCTION|PARISH|DOG|INTERNATIONAL|COMPANY|COMPANY|EDUCATION|UNIVERSITY|SCHOOL|TEACHER|FINANCE|BUILDER|BUSINESS|BANK|PROJECT|CHURCH|ESTATE AGENT|MANUFACT|SALE|SPORT|FARM|CLUB",
        "CONTRACTOR|CIVIL SERV.*|CLERICAL|COUNCIL|MEDICAL SCHOOL|ACCOUNT|CARER|CHARITY",
    ],
)
hc_support = match_with_exclusions(
    [
        "SUP+ORT *WORKER",
        "HOSPITAL|HEALTH *CARE|MENTAL *HEALTH|MATERNITY|CLINICAL|WARD|NURSE|NURSING(?! *HOME)|SURGERY|A&E|ONCOLOGY|PHLEBOTOM|AMBULANCE|WARD|MIDWIFE|ACCIDENT *(& *|AND *)*EMERGENCY|COVID.*SWA[BP]",
    ],
    ["BU(IS|SI)NESS SUPPORT", "LOCAL COUNCIL|DISCHARGE|POST HOSPITAL|HOME NURS", "HEALTH *CARE ASSIST|\\bHCA\\b"],
)
domestic = match_with_exclusions(
    "(HOME|HOUSE|DOMESTIC) *CARE|CARER* OF HOME|HOUSE *WIFE|HOME *MAKER",
    "(?<!MENTAL )HEALTH *CARE|CRITICAL CARE|(?<!NO[NT][ -])MEDICAL|DONOR CARER*|HOSPITAL",
)
child_care = match_with_exclusions(
    "CHILD *(CARE|MIND)|NANN[YIE]+\\b|AU PAIR", "(?<!MENTAL )HEALTH *CARE|CRITICAL CARE|MEDICAL|DONOR CARER*|HOSPITAL"
)
informal_care = match_with_exclusions(
    "(((CAR(ER|ING)+|NURSE) (FOR|OF))|LOOKS* *AFTER) *(MUM|MOTHER|DAD|FATHER|SON|D[AU]+GHT|WIFE|HUSB|PARTNER|CHILD|FAM|T*H*E* ELDERLY)",
    "(?<!MENTAL )HEALTH *CARE|CRITICAL CARE|MEDICAL|DONOR CARER*|HOSPITAL",
)
house_care = match_with_exclusions(
    "(HOME|HOUSE|DOMESTIC) *CARE|CARER* OF HOME|HOUSE *WIFE|HOME *MAKER",
    "(?<!MENTAL )HEALTH *CARE|CRITICAL CARE|(?<!NO[NT][ -])MEDICAL|DONOR CARER*|HOSPITAL",
)
formal_care = match_with_exclusions(
    "^CAE?RE*R *(CARE*|NA)*$|(CARE|NURSING) *HOME|(SOCIAL|COMMUNITY|DOMICIL[IA]*RY)* *CARE|CARE *(WORK|ASSISTANT)|ASST CARING|CARE SUPPORT WORK|SUPPORT *WORKER *CARE|INDEPEND[EA]NT LIVING",
    "(?<!MENTAL )HEALTH *CARE|CRITICAL CARE|MEDICAL|DONOR CARER*|HOSPITAL",
)
pharmacist = match_with_exclusions(
    ["PHARMA(?![CS][EU]*TIC)", "AS+IST|TECHN|RETAIL|DISPEN[SC]|SALES AS+IST|HOSPITAL|PRIMARY CARE|SERVE CUSTOM"],
    "ANALYST|CARE HOME|ELECTRICAL|COMPAN(Y|IES)|INDUSTR|DIRECTOR|RESEARCH|WAREHOUSE|LAB|PROJECT|PRODUCTION|PROCESS|LAB|QA|QUALITY",
)
dietician = match_with_exclusions("\\bD[EIA]{0,2}[TC][EI]?[CT]+[AEIOU]*[NC(RY)]|\\bDIET(RIST)?\\b", "DETECTION")
doctor = match_with_exclusions(
    "DOCT[EO]R|\\bGP\\b|GENERAL PRACTI[CIAN|TION]|\\bDR\\b|CARDIAC|\\A ?(&|AND) ?E\\|PHYSI[CT]I[AO]",
    "LECTURER|DOCTORI*AL|RESEARCH|PHD|STAT|WAR|ANIMAL|SALES*|FINANCE",
)
dentist = "DENTIS.*|\\bDENTAL"
midwife = "MI*D*.?WI*F.?E.?|MIDWIV|MID*WIF|HEALTH VISITOR"
nurse = match_with_exclusions(
    "N[IU]RS[EY]|MATRON|\\bHCA\\b", "N[UI][RS]S[EA]R*[YIEU]+|(CARE|NURSING) *HOME|SCHOOL|TEACHER"
)
paramedic = match_with_exclusions("PARA *MEDIC|AMBUL[AE]NCE", teaching_exclusions)
additional_hc = match_with_exclusions(
    "SONOGRAPHER|RADIO(GRAPHER|LOGIST)|VAC+INAT[OE]R|(ORTHO(PAEDIC)?|\\bENT CONSULTANT|ORAL|EYE)+ SURGEON|SURGEON SURGERY|(DIABETIC EYE|RETINAL) SCRE+NER|(PH|F)LEBOTOM|CLINICAL SCIEN|"
    + "MEDICAL PHYSICIST|CARDIAC PHYSIOLOG|OSTEOPATH|OPTOMOTRIST|PODIATRIST|OBSTETRI|GYNACOLOG|ORTHO[DOENT]+|OPTI[TC]I[AO]N|CRITICAL CARE PRACTITIONER|HOSPITAL PORTER|AN[AE]STHET[IST|IC|IA]"
    + "PALLIATIVE|DISTRICT NURS|PAEDIATRI[CT]I[AO]N|HAEMATOLOGIST",
    "LAB MANAGER",
)
covid_test = match_with_exclusions(["COVID", "TEST|SWAB|VAC+INAT|IM+UNIS|SCREEN|WARD"], "LAB|AN[AY]LIST|SCHOOL|MANAGER")
physiotherapist = match_with_exclusions(
    "PH[YI]+SIO|PH[YSIH]+IO\\s*THERAPIST|PH[YI]S[IY]CAL\\s*REHAB|PH[YI]S[IY]CAL\\s*THERAPY",
    match_with_exclusions(
        [
            "PHSYCOLOGY|PHYCOLOGIST|PYCHLOLOGIST|PHYCOLOGIST|PSYCHOLOGOLOGIST|PHYCOLIGIST|PYSCOLOGIST|PHYSCHOLOGICAL|PSYCHOLOGICIST|PSYCHOLGIST|PSYCHOLOGIST|PSYCHOLOGY|PSYCHOLOGICAL",
            "EDUCATION|SCHOOL|BUSINESS|STUDYING|LECTURER|PROFESSOR|ACADEMIC|RESEARCH|UNIVERSITY|TEACHING|TEACH|STUDENT|TECHNICIAN|DOCTORATE|PHD|POSTDOCTORAL|PRISON|OCCUPATION|OCCUPATIONAL|FORENSICS|\\bUCL\\b",
        ]
    ),
)
social_work = "SOCIAL.*WORK|FOSTER CARE"
call_operator = match_with_exclusions(
    [
        "111|119|999|911|NHS|TRIAGE|EMERGENCY",
        "ADVI[SC][OE]R|RESPONSE|OPERAT|CALL (HANDLER|CENT(RE|ER)|TAKE)|(TELE)?PHONE|TELE(PHONE)?|COVID",
    ],
    "CUSTOMER SERVICE|SALES",
)

# patient facing
patient_facing_negative_regex = "|".join(
    [
        hc_admin,
        hc_secretary,
        call_operator,
        match_with_exclusions(
            "ONLINE|ZOOM|MICROSOFT|MS TEAMS|SKYPE|GOOGLE HANGOUTS?|REMOTE|VIRTUAL|(ONLY|OVER THE) (TELE)?PHONE|((TELE)?PHONE|VIDEO) (CONSULT|CALL|WORK|SUPPORT)|(NO[TN]( CURRENTLY)?|NEVER) (IN PERSON|FACE TO FACE)|SH[EI]+LDING|WORK(ING)? (FROM|AT) HOME|HOME ?BASED|DELIVER(Y|ING)? PRESCRI",
            "(?<!NOT )OFFICE BASED",
        ),
    ]
)
patient_facing_positive_regex = "|".join(
    [
        paramedic,
        additional_hc,
        covid_test,
        "PALLIATIVE CARE|(?<!NOT )PATI[EA]NT FACING|(LOOK(S|ING)? AFTER|SEES?|CAR(E|ING) (OF|FOR)) PATI[EA]NTS|(?<!NO )FACE TO FACE|(?<!NOT )FACE TO FACE",
        "(?<!NO )(DIRECT )?CONTACT WITH PATI[EA]NTS|CLIENTS COME TO (HER|HIS|THEIR) HOUSE",
    ]
)

healthcare_negative_regex = "|".join(
    [
        transport_exclusions,
        catering_exclusions,
        teaching_exclusions,
        media_exclusions,
        retail_exclusions,
        domestic_exclusions,
        construction_exclusions,
        religious_exclusions,
        it_exclusions,
        public_service_exclusions,
        vet,
        house_care,
        child_care,
        informal_care,
        formal_care,
        social_work,
    ]
)
healthcare_positive_regex = "|".join(
    [
        hc_admin,
        hc_secretary,
        hc_receptionist,
        hc_counsellor,
        hc_support,
        pharmacist,
        call_operator,
        patient_facing_positive_regex,
        dietician,
        doctor,
        dentist,
        midwife,
        nurse,
        paramedic,
        physiotherapist,
    ]
)

social_care_positive_regex = "|".join(["SUP+ORT *WORKER", house_care, informal_care, child_care, formal_care])

social_care_pattern = RegexPattern(
    positive_regex_pattern=social_care_positive_regex, negative_regex_pattern=healthcare_positive_regex
)

healthcare_pattern = RegexPattern(
    positive_regex_pattern=healthcare_positive_regex, negative_regex_pattern=healthcare_negative_regex
)

patient_facing_pattern = RegexPattern(
    positive_regex_pattern=patient_facing_positive_regex, negative_regex_pattern=patient_facing_negative_regex
)
