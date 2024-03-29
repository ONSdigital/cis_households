"""
Various regex patterns used in the Pipeline
"""
from collections import namedtuple

# list of common job titles - these are meant to be used as negative patterns for "at school",
# "in college" or "attending university"

occupations = [
    "ADMIN",
    "ADVIS[O,E]R",
    "ASSIST[AE]NT",
    "CARE( *|-*)TAKER",
    "CATERING",
    "CHEF",
    "CHIEF",
    "CLER(K|ICAL)",
    "COACH",
    "CONSULT",
    "COOK",
    "COUN[C,S][E,I]L+OR",
    "DEPARTMENT (FOR|OF) EDUCATION",
    "DIRECTOR",
    "DRIV(ER|ING)",
    "GOVERN(MENT|ANCE)",
    "INTERN",
    "INVIGILATOR",
    "JUNIOU?R",
    "LECT[O,U]R[E,O]R",
    "MANAGE",
    "MASTER",
    "MINDER",
    "MISTRESS",
    "OFFICE(R?)",
    "P.?RS+.?N+.?L AS+IS+T+AN+",
    "PA",
    "POLICY",
    "PROF{1,2}ES{1,2}OR",
    "RECEPTION",
    "REGULATION",
    "S.?C+R+.?T+.?R+Y",
    "SALE[S?]",
    "TUTOR",
    "SUP+ORT *WORKER",
    "SUPERVISE",
    "SUPERVIS[O,E]R",
    "TEACH(ER|ING)?",
    "TRAINEE",
    "WORKER",
    "INSTRUCTOR",
]


RegexPattern = namedtuple("RegexPattern", ["positive_regex_pattern", "negative_regex_pattern"])

social_care_positive_regex = "|".join(
    [
        "((?=.*?(COUNS|COUNC))(^(?!.*(ADDICT|VICTIM|TRAUMA|MENTAL HEALTH|DRUG|ALCOHOL|ABUSE|SUBSTANCE))))",  # noqa:E501 # counsellor
        "(?<!BUSINESS )SUP+ORT *WORKER",  # non-business support
        "(((CAR(ER|ING)+|NURSE) (FOR|OF))|LOOKS* *AFTER) *(MUM|MOTHER|DAD|FATHER|SON|D[AU]+GHT|WIFE|HUSB|PARTNER",
        "CHILD|FAM|T*H*E* ELDERLY)",  # general carer
        "(?=.*?(CHILD (CARE|MIND)|NANN[YIE]+|AU PAIR|(CARE|NURSING) HOME|(SOCIAL|COMMUNITY|DOMICIL[IA]*RY) CARE|CARE (WORK|ASSISTANT|SUPPORT)|ASS(T|ISTED) CARING|SUPPORT WORKER|INDEPEND[EA]NT LIVING))(^(?!.*(HOSPITAL|MEDICAL|CRITICAL|MENTAL)))",  # noqa: E501
        "SOCIAL.*WORK|FOSTER CARE",  # social worker
    ]
)
healthcare_roles = "|".join(
    [
        "CAMHS",
        "X.?RAY",
        "DR",
        "((?=.*?(DOCT[EO]R|GP|GENERAL PRACTI[CIAN|TION]|DRb|CARDIAC",
        "A ?(&|AND) ?E|PHYSI[CT]I[AO]))(^(?!.*(LECTUR|DOCTORI*AL",
        "RESEARCH|PHD|STAT|WAR|ANIMAL|SALES*|FINANCE))))",  # DOCTOR
        "((?=.*?(N[IU]RS(E|ING)|MATRON|HCA))(^(?!.*(HOME|SCHOOL|TEACHER))))",
        "PA?EDIATRIC",
        "NHS",
        "OSTEOPATH",
        "OUTPATIENT",
        "(?<!POST )HOSPITAL(?!ITY)",
        "MEDIC[A-Z]*",
        "SURG[EA]RY",
        "CLINIC",
        "HEALTH *CARE",
        "MENTAL *HEALTH",
        "DENTAL",
        "DENTIST",
        "GP",
        "OPTI[A-Z]*",
        "CHIROPRAC",
        "A&E",
        "MATERNITY",
        "WARD",
        "PHLEBOTOM",
        "((?=.*?(PARA *MEDIC|AMBUL[AE]NCE))(^(?!.*(LECTUR))))",
        "MI*D*.?WI*F.?E.?|MIDWIV|MID*WIF|HEALTH VISITOR",
        "ONCOLOGY",
        "ACCIDENT *(& *|AND *)*EMERGENCY",
        "COVID.*SWA[BP]",
        "PHARMA(?![CS][EU]*TIC)",
        "((?=.*?(PH[YI]+SIO|PH[YSIH]+IO*THERAPIST|PH[YI]S[IY]CAL*REHAB",
        "PH[YI]S[IY]CAL*THERAPY))(^(?!.*(PHYSIOLOG|PHYSIOSIST))))",
        "((?=.*?(D[EIA]{0,2}[TC][EI]?[CT]+[AEIOU]*[NC(RY)]|DIET(RIST)?))(^(?!.*(DETECTION))))",  # DIETICIAN,
        "((?=.*?(COUN(C|S)))(?=.*?(ADDICT|VICTIM|TRAUMA|MENTAL HEALTH|DRUG|ALCOHOL|ABUSE|SUBSTANCE)))",
    ]
)
additional_healthcare_roles = "|".join(
    [
        "SONOGRAPHER",
        "RADIO(GRAPHER|LOGIST)",
        "VAC+INAT[OE]R",
        "ORTHO(PAEDIC)?",
        "ENT CONSULTANT",
        "(ORAL|EYE)+ SURGEON",
        "SURGE(RY|ON)",
        "(DIABETIC|EYE|RETINAL) SCRE+NER",
        "(PH|F)LEBOTOM",
        "CLINICAL SCIEN",
        "MEDICAL PHYSICIST" "CARDIAC PHYSIOLOG",
        "OSTEOPATH",
        "OPTOMOTRIST",
        "PODIATRIST",
        "OBSTETRI",
        "GYNACOLOG",
        "ORTHO[DOENT]+",
        "OPTI[TC]I[AO]N",
        "CRITICAL CARE PRACTITIONER",
        "HOSPITAL PORTER",
        "AN[AE]STHET(IST|IC|IA)",
        "PALLIATIVE",
        "DISTRICT NURS",
        "PAEDIATRI[CT]I[AO]N",
        "HAEMATOLOGIST",
    ]
)

non_healthcare_regex = "|".join(
    [
        "DRIV(E|ER|ING)|PILOT|TRAIN DRIVE|TAXI|LORRY|TRANSPORT|DELIVER|SUPPLY",  # delivery
        "CHEF|SOUS|COOK|CATER|BREWERY|CHEESE|KITCHEN|KFC|CULINARY|FARM(ER|ING)",  # catering
        "AC[AE]DEMIC|RESEAR*CH|SCIEN|LAB(ORATORY)?|DATA|ANAL|STATIST|EPIDEMI|EXAM|EDUCAT|EARLY YEARS|SCHOOL|COLL.GE",
        "TEACH|LECTURE|PROFESS|HOUSE *(M[AI]ST(ER|RESS)|PARENT)|COACH|TRAINER|INSTRUCT|TUTOR|LEARN|EDUCATION|SCHOOL|UNIV",  # noqa: E501 education
        "BROADCAST|JOURNALIST|CAMERA|WRIT|COMMUNICAT|CURAT(OR)*|MARKETING|MUSICIAN|ACT([OE]R|RESS)|ARTIST",  # noqa:E501 media/reporting
        "RETAIL|BUYER|SALE|BUY AND SELL|CUSTOMER|AGENT|BANK(ING|ER)|INSURANCE|BEAUT(Y|ICIAN)?|NAIL|HAIR|SHOP|PROPERTY|TRADE|SUPER *MARKET|WH *SMITH|TESCO",  # noqa:E501 retail
        "LOCAL *GOVERNMENT|CIVIL *SERV(ANT|ICE)|HOTEL|CARE *HOME|VET[A-Z]*RY|HAIR *(SALON|DRESS)+|SPORT[S ]*CENT|LEISURE|BEAUTY|CLUB|PROJECT|BUSINESS|COMPAN(Y|IES)",  # noqa:501
        "SPA|RETAIL|LAW|LEGAL|BAR WORK|GARAGE|VET|FINANCE|INTERNATI|SOLICITOR|PRODUCT|CARER|ACCOUNT|ANALYST|ELECTRICAL|INDUSTR|RESEARCH|WAREHOUSE|PRODUCTION|PROCESS",  # noqa: E501
        "QA|QUALITY",
        "(?=.*?(VETS*|VEN?T[A-Z]*(RY|IAN)|EQUIN|(DOG|CAT)|HEDGEHOG|ANIMAL))(^(?!.*(VET PEOPLE)).*)",  # vet
        "DOMESTIC|CLEAN|LAU*ND.*Y",  # domestic
        "BUILD|CONSTRUCT|RENOVAT|REFIT|ENGINE|PLANT|CR[AI]*NE*|SURVEY(OR)*|DESIGNER|ARCHITECT|TECHNICIAN|MECHAN|MANUFACT|ELECTRIC|CARPENTER",  # noqa:501 tradesmen
        "CHAPL[AI]*N|VICAR|CLERGY|MINISTER|PREACH|CHURCH|PARISH",  # religous services
        "I[ \\.]*T\\.?\\b|DIGIT|WEBSITE|NETWORK|DEVELOPER|SOFTWARE|SYSTEM|CLERICAL",  # IT
        "CHAIR|CHARITY|CITIZEN|CIVIL|VOLUNT|LIBRAR|TRANSLAT|INVESTIGAT|FIRE ?(WO)?(M[AE]N|FIGHT)|POLICE|POST *(WO)*MAN|PRISON|FIRST AID|SAFETY|TAX|GOVER?NMENT",  # noqa:E501 public service
        "CONTRACTOR|COUNCIL|MEDICAL SCHOOL|^RETIRED",
    ]
)

non_patient_facing_regex = "|".join(
    [
        "ONLINE|ZOOM|MICROSOFT|MS TEAMS|SKYPE|GOOGLE HANGOUTS?|REMOTE|VIRTUAL",
        "(ONLY|OVER THE) (TELE)?PHONE|((TELE)?PHONE|VIDEO) (CONSULT|CALL|WORK|SUPPORT)",
        "(NO[TN]( CURRENTLY)?|NEVER) (IN PERSON|FACE TO FACE)",
        "SH[EI]+LDING|WORK(ING)? (FROM|AT) HOME|HOME ?BASED|DELIVER(Y|ING)? (PRE|PER)SCRI",
        "(?<!NOT )OFFICE BASED",
        "((?=.*?(111|119|999|911|NHS|TRIAGE|EMERGENCY))(?=.*?(ADVI[SC][OE]R|RESPONSE|OPERAT|CALL (HANDLER|CENT(RE|ER)|TAKE)",  # noqa:E501
        "(TELE)?PHONE|TELE(PHONE)?|COVID))(^(?!.*(CUSTOMER SERVICE|SALES)).*))",  # call handler
        "RECEPTION",
        "S.?C+R+.?T+.?R+Y",
        "P.?RS+.?N+.?L AS+IS+T+AN+",
        "CLER(K|ICAL)",
        "ADMIN|MANAGER",
    ]
)

outpatient_exclusions = "|".join(["LOCAL COUNCIL", "DISCHARGE", "BUSINESS"])
support_roles = "|".join(
    [
        "RECEPTION",
        "ASSIST[AE]NT",
        "S.?C+R+.?T+.?R+Y",
        "PA",
        "P.?RS+.?N+.?L AS+IS+T+AN+",
        "ADMIN",
        "CLER(K|ICAL)",
        "SUP+ORT *WORKER",
        "TRAINEE",
        "JUNIOU?R",
    ]
)
healthcare_positive_regex = "|".join(
    [  # noqa: E501  # counsellor
        f"((?=.*?({support_roles}))(?=.*?({healthcare_roles}))(^(?!.*({outpatient_exclusions})).*))",  # noqa: E501 # other location dependent workers
        "((?=.*?(111|119|999|911|NHS|TRIAGE|EMERGENCY))(?=.*?(ADVI[SC][OE]R|RESPONSE|OPERAT|CALL (HANDLER|CENT(RE|ER)|TAKE)|(TELE)?PHONE|TELE(PHONE)?|COVID))(^(?!.*(CUSTOMER SERVICE|SALES)).*))",  # noqa: E501  # call handler
        "|".join(["^" + x for x in healthcare_roles.split("|")]),
    ]
)

healthcare_pattern = RegexPattern(
    positive_regex_pattern=healthcare_positive_regex,
    negative_regex_pattern=non_healthcare_regex + "|" + social_care_positive_regex,
)

socialcare_pattern = RegexPattern(
    positive_regex_pattern=social_care_positive_regex,
    negative_regex_pattern=healthcare_positive_regex,
)

patient_facing_pattern = RegexPattern(
    positive_regex_pattern=healthcare_positive_regex + "|" + additional_healthcare_roles,
    negative_regex_pattern=non_patient_facing_regex + "|" + non_healthcare_regex,
)

non_patient_facing_pattern = RegexPattern(positive_regex_pattern=non_patient_facing_regex, negative_regex_pattern=None)

work_from_home_pattern = RegexPattern(
    positive_regex_pattern="(W(K|ORK.*?) F(ROM?) H(OME?))|(WFH)",
    negative_regex_pattern="HOMELESS",
)

healthcare_bin_pattern = RegexPattern(
    positive_regex_pattern=healthcare_positive_regex + "|" + additional_healthcare_roles,
    negative_regex_pattern=non_healthcare_regex + "|" + social_care_positive_regex,
)

at_school_pattern = RegexPattern(
    positive_regex_pattern="|".join(
        [
            "(SCHOOL.+(?<=CHILD|GIRL|BOY|PUPIL|AGE))",
            "((AT|ATTEND(S|ING)|IN|GOES TO).SCHOOL)",
            "((PRIMARY|SECONDARY).(SCHOOL).?(?:YEAR)?)",
            "^(?:MINOR|CHILD)$",
            "ST[UI]D(?:YING|Y|ENT|T|WNY)",
        ]
    ),
    negative_regex_pattern="|".join(occupations),
)

at_university_pattern = RegexPattern(
    positive_regex_pattern="|".join(
        [
            "(?:IN|AT).?COLLEGE",
            "UNI\\b",
            "UNIVERSITY",
            "EDUCATION",
            "ST[UI]D(?:YING|Y|ENT|T|WNY)",
            "PHD",
        ]
    ),
    negative_regex_pattern="|".join(occupations),
)

not_working_pattern = RegexPattern(
    positive_regex_pattern="|".join(
        [
            r"(NONE|NOTHING|NIL|AT HOME)",
            r"(NO.{0,}WORK)",
            r"(?=UN(ABLE|EMPLOY))",
            r"((SONS|TERS|THERS|'S).CARER)",
            r"(TERNITY.(?=LEAVE))",
            r"(FULL.?TIME.{0,}(MUM|MOM|MOTHER|DAD|FATHER))",
            r"(HOME|HOUSE)\s*(?=WIFE|MAKER|HUSBAND|PARENT)",
            r"OFF SICK|SICK LEAVE",
        ]
    ),
    negative_regex_pattern="|".join(["MASTER", "MISTRESS", "COVER"]),
    # COVER above will suppress things such as "PATERNITY LEAVE COVER"
)

self_employed_regex = RegexPattern(positive_regex_pattern="SELF.?EMPLOYED", negative_regex_pattern=None)
retired_regex_pattern = RegexPattern(
    positive_regex_pattern="RE[TFIER]{2,}(ED|RD)(?!( (PEOPLE|MILITARY)))",
    negative_regex_pattern="(SEMI|PART[a-zA-Z]{3,}).?RE[TFIER]{2,}(ED|RD)(?!( (PEOPLE|MILITARY)))",
)

furloughed_pattern = RegexPattern(
    positive_regex_pattern="FU[RL]{1,3}O[UW]{0,1}[GHE]{1,}D?",
    negative_regex_pattern="|".join(["NOT ON FURLOUGH", "FURLOUGHED ON AND OFF CURRENTLY WORKING"]),
)


in_college_or_further_education_pattern = RegexPattern(
    positive_regex_pattern="|".join(
        [
            "[AT].?LEVELS?",
            "YEAR \\d{2}",
            "VOCATION",
            "QUALIFICATION",
            "SIXTH FORM",
            "COLLEGE",
            "ST[UI]D(?:YING|Y|ENT|T|WNY)",
        ]
    ),
    negative_regex_pattern="|".join(occupations + ["SCHOOL"]),
)


childcare_pattern = RegexPattern(
    positive_regex_pattern="|".join(
        [
            "NU[RS].+[RE]Y",
            "DAY.?CARE",
            "CHILD.?CARE",
            "CHILD.?MINDER",
            "PLAY.?GROUP",
            "CRECHE",
            "PRE.?SCHOOL",
            "LEARNER",
            "EDUCATION",
            "STUDENT",
        ]
    ),
    # below MINDER is in the list of occupations but we want that not to match with
    # CHILDMINDER that's why we are excluding MINDER from the -ve pattern list
    negative_regex_pattern="|".join([i for i in occupations if i not in ["MINDER"]]),
)
