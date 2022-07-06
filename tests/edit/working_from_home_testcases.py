# A list of positive test cases which need to be interpreted as "Working from Home" &
# negative test cases which shouldn't.
# Please append new cases to the appropriate list below
test_data = {
    "positive": [
        "WORKING FROM HOME",
        "WORKS FROM HOME",
        "WORK FROM HOME",
        "NO CHANGE IN WORK - BOTH STILL WORKING FROM HOME",
        "NO CHANGE IN WORK - CONTINUING TO WORK FROM HOME",
        "WORKING FROM HOME - ENVIRONMENT AGENCY - NO CONTACT WITH COLLEAGUES",
        "WORKING FROM HOME - NO CHANGE IN CIRCUMSTANCES",
        "WORKING FROM HOME FULL TIME - SEE PREVIOUS NOTES",
        "WORKING FROM HOME ON COMPUTER",
        "WORKING FULL TIME FRO HOME",
        "WORKING FULL TIME FROM HOME - SOFTWARE ENGINEER",
        "WORKING FULL TIME FROM HOME IT",
        "WK FROM HOM",
        "WFH",
    ],
    "negative": [
        "WORK WORK WORK",
        "SWEET HOME ALABAMA",
        "FROM DAWN TO DUSK",
        "WORK DAY",
        "WORKING 9 TO 5",
        "HOME WORK",
    ],
}
