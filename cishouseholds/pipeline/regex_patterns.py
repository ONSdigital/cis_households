"""
Various regex patterns used in the Pipeline
"""
from collections import namedtuple

RegexPattern = namedtuple("RegexPattern", ["positive_regex_pattern", "negative_regex_pattern"])

social_care_positive_regex = [
    "COUNS|COUNC",  # counsellor
    "(?<!BUSINESS )SUP+ORT *WORKER",  # non-business support
    "CHILD *(CARE|MIND)|NANN[YIE]+|AU PAIR",  # child carer
    "(((CAR(ER|ING)+|NURSE) (FOR|OF))|LOOKS* *AFTER) *(MUM|MOTHER|DAD|FATHER|SON|D[AU]+GHT|WIFE|HUSB|PARTNER|CHILD|FAM|T*H*E* ELDERLY)",  # general carer
    "^CAE?RE*R *(CARE*|NA)*$|(CARE|NURSING) *HOME|(SOCIAL|COMMUNITY|DOMICIL[IA]*RY)* *CARE|CARE *(WORK|ASSISTANT)|ASS(T|ISTED) CARING|CARE SUPPORT WORK|SUPPORT *WORKER *CARE|INDEPEND[EA]NT LIVING",
    "SOCIAL.*WORK|FOSTER CARE",  # social worker
]

healthcare_positive_regex

social_care_pattern = RegexPattern(
    positive_regex_pattern="|".join(social_care_positive_regex),
    negative_regex_pattern="|".join(healthcare_positive_regex),
)

patient_facing_pattern = RegexPattern(
    positive_regex_pattern="|".join(
        [
            "(COVID (TEST|SWAB|VAC+INAT|IM+UNIS|SCREEN|WARD) (?!(LAB|AN[AY]LIST|SCHOOL|MANAGER)))",  # patient facing covid tester
            "PALLIATIVE CARE|(?<!NOT )PATI[EA]NT FACING|(LOOK(S|ING)? AFTER|SEES?|CAR(E|ING) (OF|FOR)) PATI[EA]NTS|(?<!NO )FACE TO FACE|(?<!NOT )FACE TO FACE",  # patient facing healthcare
            "(?<!NO )(?<!NO DIRECT )CONTACT WITH PATI[EA]NTS|\w+ COME TO \w+ HOUSE",  # patients come to house
            "PARA *MEDIC|AMBUL[AE]NCE",  # ambulance worker
            "SONOGRAPHER|RADIO(GRAPHER|LOGIST)|VAC+INAT[OE]R|(ORTHO(PAEDIC)?|\\bENT CONSULTANT|ORAL|EYE)+ SURGEON|SURGEON SURGERY|(DIABETIC EYE|RETINAL) SCRE+NER|(PH|F)LEBOTOM|CLINICAL SCIEN",
            "MEDICAL PHYSICIST|CARDIAC PHYSIOLOG|OSTEOPATH|OPTOMOTRIST|PODIATRIST|OBSTETRI|GYNACOLOG|ORTHO[DOENT]+|OPTI[TC]I[AO]N|CRITICAL CARE PRACTITIONER|HOSPITAL PORTER|AN[AE]STHET[IST|IC|IA]",
            "PALLIATIVE|DISTRICT NURS|PAEDIATRI[CT]I[AO]N|HAEMATOLOGIST",
        ]
    ),
    negative_regex_pattern="|".join(
        [
            "ADMIN(?!IST[EO]R)|ADM[A-Z]{2,}RAT[EO]R|CLERICAL|CLERK",  # administrative worker
            "S.?C+R+.?T+.?R+Y|\\sPA\\s|P.?RS+.?N+.?L AS+IS+T+AN+",  # secretary or pa
            "(ADVI[SC][OE]R|RESPONSE|OPERAT|CALL (HANDLER|CENT(RE|ER)|TAKE)|(TELE)?PHONE)",  # phone work
            "ONLINE|ZOOM|MICROSOFT|MS TEAMS|SKYPE|GOOGLE HANGOUTS?|REMOTE|VIRTUAL|(ONLY|OVER THE) (TELE)?PHONE|((TELE)?PHONE|VIDEO) (CONSULT|CALL|WORK|SUPPORT)",
            "(NO[TN]( CURRENTLY)?|NEVER) (IN PERSON|FACE TO FACE)|SH[EI]+LDING|WORK(ING)? (FROM|AT) HOME|HOME ?BASED|DELIVER(Y|ING)? (PRE|PER)SCRI",
            "(?<!NOT )OFFICE BASED",
        ]
    ),
)

work_from_home_pattern = RegexPattern(
    positive_regex_pattern="(W(K|ORK.*?) F(ROM?) H(OME?))|(WFH)|HOME BASED",
    negative_regex_pattern=None,
)


at_school_pattern = RegexPattern(
    positive_regex_pattern="|".join(
        [
            "(SCHOOL.+(?<=CHILD|GIRL|BOY|PUPIL|AGE))",
            "((AT|ATTEND(S|ING)|IN|GOES TO).SCHOOL)",
            "((PRIMARY|SECONDARY).(SCHOOL).?(?:YEAR)?)",
            "^(?:MINOR|CHILD)$",
        ]
    ),
    negative_regex_pattern="|".join(
        [
            "TEACH(ER|ING)?",
            "MINDER",
            "ASSISTANT",
            "MANAGER",
            "CATERING",
            "MASTER",
            "MISTRESS",
            "SUPERVISE",
            "CHIEF",
            "INVIGILATOR",
        ]
    ),
)

at_university_pattern = RegexPattern(
    positive_regex_pattern="|".join(
        [
            "(?:IN|AT).?COLLEGE",
            "UNI\\b",
            "UNIVERSITY",
            "FULL.?TIME",
            "EDUCATION",
            "ST[UI]D(?:YING|Y|ENT|T|WNY)",
        ]
    ),
    negative_regex_pattern="|".join(
        [
            "TEACH(ER|ING)?",
            "ASSISTANT",
            "MANAGER",
            "CATERING",
            "PROFESSOR",
            "LECTURER",
            "SUPERVISE",
            "CHIEF",
            "INVIGILATOR",
        ]
    ),
)

not_working_pattern = RegexPattern(
    positive_regex_pattern="|".join(
        [
            r"(NONE|NOTHING|NIL|AT HOME)",
            r"(NO.{0,}WORK)|(^UN(ABLE|EMPLOY))",
            r"((SONS|TERS|THERS|'S).CARER)",
            r"(TERNITY.LEAVE$)|((HOME|HOUSE)\w)",
            r"(FULL TIME.{0,}(MOM|MOTHER|DAD|FATHER))",
        ]
    ),
    negative_regex_pattern="|".join(["MASTER", "MISTRESS"]),
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
        ["[AT].?LEVELS?", "YEAR \\d{2}", "APPRENTICE", "VOCATION", "QUALIFICATION", "SIXTH FORM", "COLLEGE"]
    ),
    negative_regex_pattern="|".join(
        [
            "ASSISTANT",
            "LECTURER",
            "PROFESSOR" "SCHOOL",
            "INTERN",
            "TEACHER",
            "WORKER",
            "SUPERVISE",
            "CHIEF",
            "INVIGILATOR",
        ]
    ),
)
