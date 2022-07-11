"""
Various regex patterns used in the Pipeline
"""
from collections import namedtuple

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
    negative_regex_pattern="|".join(
        [
            "TEACH(ER|ING)?",
            "MINDER",
            "ASSISTANT",
            "MANAGER",
            "CATERING",
            "MASTER",
            "MISTRESS",
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
