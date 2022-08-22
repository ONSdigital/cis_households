"""
Various regex patterns used in the Pipeline
"""
from collections import namedtuple

# list of common job titles - these are meant to be used as negative patterns for "at school",
# "in college" or "attending university"
occupations = [
    "ASSISTANT",
    "CATERING",
    "CHIEF",
    "INTERN",
    "INVIGILATOR",
    "LECTURER",
    "MANAGER",
    "MASTER",
    "MINDER",
    "MISTRESS",
    "PROFESSOR",
    "SUPERVISE",
    "TEACH(ER|ING)?",
    "WORKER",
]

RegexPattern = namedtuple("RegexPattern", ["positive_regex_pattern", "negative_regex_pattern"])

work_from_home_pattern = RegexPattern(
    positive_regex_pattern="(W(K|ORK.*?) F(ROM?) H(OME?))|(WFH)",
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
    negative_regex_pattern="|".join(occupations),
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
    negative_regex_pattern="|".join(occupations),
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
        [
            "[AT].?LEVELS?",
            "YEAR \\d{2}",
            "APPRENTICE",
            "VOCATION",
            "QUALIFICATION",
            "SIXTH FORM",
            "COLLEGE",
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
