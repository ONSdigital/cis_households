from datetime import datetime

yes_no_choice = ["Yes", "No"]
yes_no_none_choice = ["Yes", "No", None]


start_date_list = datetime(2022, 1, 1)
end_date_list = datetime(2022, 1, 10)


def get_swab_data_description(_):
    return lambda: {  # noqa: E731
        "Sample": _("random.custom_code", mask="ONS########", digit="#"),
        "Result": _("choice", items=["Negative", "Positive", "Void"]),
        "Date Tested": _("datetime.formatted_datetime", fmt="%Y-%m-%d %H:%M:%S UTC", start=2018, end=2022),
        "Lab ID": _("choice", items=["GLS"]),
        "testKit": _("choice", items=["rtPCR", None]),
        "CH1-Target": _("choice", items=["ORF1ab", None]),
        "CH1-Result": _("choice", items=["Inconclusive", "Negative", "Positive", "Rejected"]),
        "CH1-Cq": _("float_number", start=10.0, end=40.0, precision=12),
        "CH2-Target": _("choice", items=["N gene", None]),
        "CH2-Result": _("choice", items=["Inconclusive", "Negative", "Positive", "Rejected"]),
        "CH2-Cq": _("float_number", start=10.0, end=40.0, precision=12),
        "CH3-Target": _("choice", items=["S gene", None]),
        "CH3-Result": _("choice", items=["Inconclusive", "Negative", "Positive", "Rejected"]),
        "CH3-Cq": _("float_number", start=10.0, end=40.0, precision=12),
        "CH4-Target": _("choice", items=["S gene", None]),
        "CH4-Result": _("choice", items=["Positive", "Rejected"]),
        "CH4-Cq": _("float_number", start=15.0, end=30.0, precision=12),
    }


def get_blood_data_description(_, target):
    return lambda: {  # noqa: E731
        "Serum Source ID": _("random.custom_code", mask="ONS########", digit="#"),
        "Blood Sample Type": _("choice", items=["Venous", "Capillary"]),
        "Plate Barcode": _("random.custom_code", mask=f"ONS_######C{target}-#", digit="#"),
        "Well ID": _("random.custom_code", mask="@##", char="@", digit="#"),
        "Detection": _("choice", items=["DETECTED", "NOT detected", "failed"]),
        "Monoclonal quantitation (Colourimetric)": _("float_number", start=0.0, end=3500, precision=4),
        "Monoclonal bounded quantitation (Colourimetric)": _("float_number", start=20, end=400, precision=1),
        "Monoclonal undiluted quantitation (Colourimetric)": _("integer_number", start=0, end=20000),
        "Date ELISA Result record created": _("datetime.formatted_datetime", fmt="%Y-%m-%d", start=2018, end=2022),
        "Date Samples Arrayed Oxford": _("datetime.formatted_datetime", fmt="%Y-%m-%d", start=2018, end=2022),
        "Date Samples Received Oxford": _("datetime.formatted_datetime", fmt="%Y-%m-%d", start=2018, end=2022),
        "Voyager Date Created": _("datetime.formatted_datetime", fmt="%Y-%m-%d %H:%M:%S", start=2018, end=2022),
    }


def get_historic_blood_data_description(_):
    return lambda: {  # noqa: E731
        "ons_id": _("random.custom_code", mask="ONS########", digit="#"),
        "Blood Sample Type": _("choice", items=["Venous", "Capillary"]),
        "Plate Barcode": _("random.custom_code", mask="ONS_######", digit="#"),
        "Well ID": _("random.custom_code", mask="&##", digit="#", char="&"),
        "Detection": _("choice", items=["Positive", "Negative", "Could not process", "Insufficient sample", None]),
        "Monoclonal quantitation (Colourimetric)": _("random.uniform", a=0, b=150000, precision=6),
        "Date ELISA Result record created": _("datetime.formatted_datetime", fmt="%Y-%m-%d", start=2018, end=2022),
        "Date Samples Arrayed Oxford": _(
            "datetime.formatted_datetime", fmt="%Y-%m-%d %H:%M:%S UTC", start=2018, end=2022
        ),
        "Date Samples Received Oxford": _("datetime.formatted_datetime", fmt="%Y-%m-%d", start=2018, end=2022),
        "Voyager Date Created": _("datetime.formatted_datetime", fmt="%Y-%m-%d", start=2018, end=2022),
        "siemens_interpretation": _("choice", items=["Positive", "Negative", "Insufficient sample", None]),
        "tdi_assay_net_signal": _("random.randint", a=100000, b=14000000),
        "siemens_reading": _("choice", items=[str(_("random.uniform", a=0, b=10, precision=2)), "< 0.05", "> 10.00"]),
        "lims_id": _("random.custom_code", mask="ONS########", digit="#"),
    }


def get_voyager_2_data_description(_, blood_barcodes, swab_barcodes):

    return lambda: {  # noqa: E731
        "ons_household_id": _("random.custom_code", mask="############", digit="#"),
        "Visit_ID": _(
            "choice",
            items=[
                _("random.custom_code", mask="DVH-##########", digit="#"),
                _("random.custom_code", mask="DHVF-##########", digit="#"),
            ],
        ),
        "Visit Status": _(
            "choice", items=["Completed", "Dispatched", "Household did not attend", "Partially Completed", "Withdrawn"]
        ),
        "Participant_Visit_status": _(
            "choice", items=[None, "Cancelled", "Completed", "Patient did not attend", "Re-scheduled", "Scheduled"]
        ),
        "Participant_status": _("choice", items=["Active", "Completed", "Withdrawn"]),
        "Withdrawal_reason": _(
            "choice",
            items=[
                None,
                "Bad experience with tester / survey",
                "Moving location",
                "No longer convenient",
                "No longer wants to take part",
                "Participant does not want to self swab",
                "Swab / blood process to distressing",
                "Too many visits",
            ],
        ),
        "Type_of_Visit": _("choice", items=["First Visit", "Follow-up Visit"]),
        "Visit_Order": _(
            "choice",
            items=[
                None,
                "First Visit",
                "Follow-up 1",
                "Follow-up 2",
                "Follow-up 3",
                "Follow-up 4",
                "Month 10",
                "Month 11",
                "Month 12",
                "Month 13",
                "Month 14",
                "Month 15",
                "Month 18",
                "Month 2",
                "Month 3",
                "Month 4",
                "Month 5",
                "Month 6",
                "Month 7",
                "Month 8",
                "Month 9",
            ],
        ),
        "Work_Type_Picklist": _("choice", items=[None, "Blood and Swab", "Fingerprick and Swab", "Swab Only"]),
        # Should follow YYYY-mm-ddTHH:MM:SS.sssZ
        "Visit_Date_Time": _(
            "custom_random.random_date",
            start=start_date_list,
            end=end_date_list,
            format="%Y-%m-%dT%H:%M:%S.%f",
        )[:-3]
        + "Z",
        "Street": _("choice", items=[None, _("address.street_name")]),
        "City": _("choice", items=[None, _("address.city")]),
        "County": _("choice", items=[None, _("address.province")]),
        "Postcode": _("choice", items=[None, _("address.postal_code")]),
        "Cohort": _("choice", items=["Blood and Swab", "Swab Only"]),
        "Fingerprick_Status": _(
            "choice", items=[None, "Accepted", "At least one person consented", "Declined", "Invited", "Not invited"]
        ),
        "Household_Members_Under_2_Years": _("choice", items=yes_no_none_choice),
        "Infant_1": _("custom_random.random_integer", lower=0, upper=8, null_percent=0.1),
        "Infant_2": _("custom_random.random_integer", lower=0, upper=8, null_percent=0.1),
        "Infant_3": _("custom_random.random_integer", lower=0, upper=8, null_percent=0.1),
        "Infant_4": _("custom_random.random_integer", lower=0, upper=8, null_percent=0.1),
        "Infant_5": _("custom_random.random_integer", lower=0, upper=8, null_percent=0.1),
        "Infant_6": _("custom_random.random_integer", lower=0, upper=8, null_percent=0.1),
        "Infant_7": _("custom_random.random_integer", lower=0, upper=8, null_percent=0.1),
        "Infant_8": _("custom_random.random_integer", lower=0, upper=8, null_percent=0.1),
        "Household_Members_Over_2_and_Not_Present": _("choice", items=[None, "Yes", "No"]),
        "Person_1": _("custom_random.random_integer", lower=9, upper=100, null_percent=0.1),
        "Person_2": _("custom_random.random_integer", lower=9, upper=100, null_percent=0.1),
        "Person_3": _("custom_random.random_integer", lower=9, upper=100, null_percent=0.1),
        "Person_4": _("custom_random.random_integer", lower=9, upper=100, null_percent=0.1),
        "Person_5": _("custom_random.random_integer", lower=9, upper=100, null_percent=0.1),
        "Person_6": _("custom_random.random_integer", lower=9, upper=100, null_percent=0.1),
        "Person_7": _("custom_random.random_integer", lower=9, upper=100, null_percent=0.1),
        "Person_8": _("custom_random.random_integer", lower=9, upper=110, null_percent=0.1),
        "Person_1_Not_Consenting_Age": _("custom_random.random_integer", lower=9, upper=110, null_percent=0.1),
        "Person1_Reason_for_Not_Consenting": _("text.quote"),
        "Person_2_Not_Consenting_Age": _("custom_random.random_integer", lower=9, upper=110, null_percent=0.2),
        "Person2_Reason_for_Not_Consenting": _("text.quote"),
        "Person_3_Not_Consenting_Age": _("custom_random.random_integer", lower=9, upper=110, null_percent=0.3),
        "Person3_Reason_for_Not_Consenting": _("text.quote"),
        "Person_4_Not_Consenting_Age": _("custom_random.random_integer", lower=9, upper=110, null_percent=0.4),
        "Person4_Reason_for_Not_Consenting": _("text.quote"),
        "Person_5_Not_Consenting_Age": _("custom_random.random_integer", lower=9, upper=110, null_percent=0.5),
        "Person5_Reason_for_Not_Consenting": _("text.quote"),
        "Person_6_Not_Consenting_Age": _("custom_random.random_integer", lower=9, upper=110, null_percent=0.5),
        "Person6_Reason_for_Not_Consenting": _("text.quote"),
        "Person_7_Not_Consenting_Age": _("custom_random.random_integer", lower=9, upper=110, null_percent=0.6),
        "Person7_Reason_for_Not_Consenting": _("text.quote"),
        "Person_8_Not_Consenting_Age": _("custom_random.random_integer", lower=9, upper=110, null_percent=0.7),
        "Person8_Reason_for_Not_Consenting": _("text.quote"),
        "Person_9_Not_Consenting_Age": _("custom_random.random_integer", lower=9, upper=110, null_percent=0.8),
        "Person9_Reason_for_Not_Consenting": _("text.quote"),
        "Participant_id": _("random.custom_code", mask="DHR-############", digit="#"),
        "Title": _("choice", items=["Dr.", "Miss.", "Mr.", "Mrs.", "Ms.", "Prof.", None]),
        "First_Name": _("person.first_name"),
        "Middle_Name": _("person.first_name"),
        "Last_Name": _("person.last_name"),
        # Format dd/mm/YYY HH:MM
        "DoB": _(
            "discrete_distribution",
            population=[_("datetime.formatted_datetime", fmt="%d/%m/%Y %H:%M", start=1980, end=2021), None],
            weights=[0.9, 0.1],
        ),
        "Email": _("choice", items=[_("person.email", domains=["gsnail.ac.uk"]), None]),
        "Have_landline_number": _("choice", items=yes_no_none_choice),
        "Have_mobile_number": _("choice", items=yes_no_none_choice),
        "Have_email_address": _("choice", items=yes_no_none_choice),
        "Prefer_receive_vouchers": _("choice", items=["Email", "Paper(Post)"]),
        "Confirm_receive_vouchers": _("choice", items=["false", "true"]),
        "No_Email_address": _("custom_random.random_integer", lower=0, upper=1),
        "Able_to_take_blood": _("choice", items=yes_no_none_choice),
        "No_Blood_reason_fingerprick": _(
            "choice",
            items=[
                "Bruising or pain after first attempt",
                "Couldn't get enough blood",
                "No stock",
                "Other",
                "Participant felt unwell/fainted",
                "Participant refused to give blood on this visit",
                "Participant time constraints",
                "Two attempts made",
                None,
            ],
        ),
        "No_Blood_reason_venous": _(
            "choice",
            items=[
                "Bruising or pain after first attempt",
                "No stock",
                "Non-contact visit. Household self-isolating",
                "Other",
                "Participant dehydrated",
                "Participant felt unwell/fainted",
                "Participant refused",
                "Participant time constraints",
                "Poor venous access",
                "Two attempts made",
                None,
            ],
        ),
        "bloods_barcode_1": _(
            "discrete_distribution",
            population=blood_barcodes,
            weights=[1 / len(blood_barcodes)] * len(blood_barcodes),
            null_prop=0.2,
        ),
        "Swab_Barcode_1": _(
            "discrete_distribution",
            population=swab_barcodes,
            weights=[1 / len(swab_barcodes)] * len(swab_barcodes),
            null_prop=0.2,
        ),
        # Format:  YYYY-mm-ddTHH:MM:SS.sssZ
        "Date_Time_Samples_Taken": _(
            "discrete_distribution",
            population=[
                _("custom_random.random_date", start=start_date_list, end=end_date_list, format="%Y-%m-%dT%H:%M:%S.%f")[
                    :-3
                ]
                + "Z",
                None,
            ],
            weights=[0.5, 0.5],
        ),
        "Sex": _("choice", items=["Female", "Male", None]),
        "Gender": _("choice", items=["Female", "Male", "Prefer not to say", None]),
        "Ethnic_group": _(
            "choice",
            items=[
                "Asian or Asian British",
                "Black or African or Caribbean or Black British",
                "Mixed/Multiple Ethnic Groups",
                "Other Ethnic Group",
                "White",
            ],
        ),
        "Ethnicity": _(
            "choice",
            items=[
                "Any other Asian background",
                "Any other Black background",
                "Any other Mixed background",
                "Any other ethnic group",
                "Any other white background",
                "Asian or Asian British-Bangladeshi",
                "Asian or Asian British-Chinese",
                "Asian or Asian British-Indian",
                "Asian or Asian British-Pakistani",
                "Black,Caribbean,African-African",
                "Black,Caribbean,Afro-Caribbean",
                "Mixed-White & Asian",
                "Mixed-White & Black African",
                "Mixed-White & Black Caribbean",
                "Other ethnic group-Arab",
                "White-British",
                "White-Gypsy or Irish Traveller",
                "White-Irish",
            ],
        ),
        "Ethnicity_Other": _("text.sentence"),  # free text field, can be null 1 to 249
        "Consent_to_First_Visit": _("choice", items=yes_no_choice),
        "Consent_to_Five_Visits": _("choice", items=yes_no_choice),
        "Consent_to_April_22": _("choice", items=yes_no_choice),
        "Consent_to_Sixteen_Visits": _("choice", items=yes_no_choice),
        "Consent_to_Blood_Test": _("choice", items=yes_no_choice),
        "Consent_to_Finger_prick_A1_A3": _("choice", items=yes_no_none_choice),
        "Consent_to_extend_study_under_16_B1_B3": _("choice", items=yes_no_none_choice),
        "Consent_to_be_Contacted_Extra_Research": _("choice", items=yes_no_choice),
        "Consent_to_be_Contacted_Extra_ResearchYN": _("choice", items=yes_no_none_choice),
        "Consent_to_use_of_Surplus_Blood_Samples": _("choice", items=yes_no_choice),
        "Consent_to_use_of_Surplus_Blood_SamplesYN": _("choice", items=yes_no_none_choice),
        "Approached_for_blood_samples?": _("choice", items=yes_no_none_choice),
        "Consent_to_blood_samples_if_positive": _("choice", items=["False", "True"]),
        "Consent_to_blood_samples_if_positiveYN": _("choice", items=yes_no_none_choice),
        "Consent_to_fingerprick_blood_samples": _("choice", items=["False", "True"]),
        "Accepted_invite_to_fingerprick": _("choice", items=yes_no_none_choice),
        "Re_consented_for_blood": _("choice", items=["False", "True"]),
        "What_is_the_title_of_your_main_job": _("text.sentence"),  # free text field, can be null 1 to 73
        "What_do_you_do_in_your_main_job_business": _("text.sentence"),  # free text field, can be null 1 to 333
        "Occupations_sectors_do_you_work_in": _(
            "choice",
            items=[
                "Armed forces",
                "Art or entertainment or recreation",
                "Arts or Entertainment or Recreation",
                "Arts or entertainment or recreation",
                "Civil Service or Local Government",
                "Financial Services (incl. insurance)",
                "Financial services (incl. insurance)",
                "Food Production and agriculture (incl. farming)",
                "Food production and agriculture (incl. farming)",
                "Health care",
                "Hospitality (e.g. hotel or restaurant or cafe)",
                "Information technology and communication",
                "Manufacturing or construction",
                "Other employment sector (specify)",
                "Other occupation sector",
                "Other occupation sector (specify)",
                "Personal Services (e.g. hairdressers or tattooists)",
                "Retail Sector (incl. wholesale)",
                "Retail sector (incl. wholesale)",
                "Social Care",
                "Social care",
                "Teaching and education",
                "Transport (incl. storage and logistic)",
                "Transport (incl. storage and logistics)",
                "Transport (incl. storage or logistic)",
                None,
            ],
        ),
        "occupation_sector_other": _("text.sentence"),  # free text field, can be null 1 to 75
        "Work_in_a_nursing_residential_care_home": _("choice", items=yes_no_none_choice),
        "Do_you_currently_work_in_healthcare": _(
            "choice",
            items=[
                None,
                "Primary care (e.g. GP, dentist)",
                "Secondary care (e.g. hospital)",
                " Other healthcare (e.g. mental health)",
            ],
        ),
        "Direct_contact_patients_clients_resid": _("choice", items=yes_no_none_choice),
        "Have_physical_mental_health_or_illnesses": _("choice", items=yes_no_none_choice),
        "physical_mental_health_or_illness_reduces_activity_ability": _(
            "choice", items=[None, "Not at all", "Yes, a little", "Yes, a lot"]
        ),
        "Have_you_ever_smoked_regularly": _("choice", items=yes_no_none_choice),
        "Do_you_currently_smoke_or_vape": _(
            "choice", items=[None, "Yes,  cigarettes", "Yes, cigar", "Yes, pipe", "Yes, vape/e-cigarettes"]
        ),
        "Do_you_currently_smoke_or_vape_at_all": _(
            "choice", items=[None, "Cigarettes", "Cigar", "Pipe", "Vape/e-cigarettes", "Hookah/shisha pipes"]
        ),
        "Smoke_Yes_cigarettes": _("choice", items=yes_no_none_choice),
        "Smoke_Yes_cigar": _("choice", items=yes_no_none_choice),
        "Smoke_Yes_pipe": _("choice", items=yes_no_none_choice),
        "Smoke_Yes_vape_e_cigarettes": _("choice", items=yes_no_none_choice),
        "Smoke_Hookah/shisha pipes": _("choice", items=yes_no_none_choice),
        "What_is_your_current_working_status": _(
            "choice",
            items=[
                None,
                "5y and older in full-time education",
                "Employed and currently working (including if on leave or sick leave for less than 4 weeks)",
                "Attending university (including if temporarily absent)"
                "Self-employed and currently working (include if on leave or sick leave for less than 4 weeks)",
            ],
        ),
        "Paid_employment": _("choice", items=yes_no_none_choice),
        "Main_Job_Changed": _("choice", items=yes_no_none_choice),
        "Where_are_you_mainly_working_now": _(
            "choice",
            items=[
                None,
                "Both (work from home and work somewhere else)",
                "From home (in the same grounds or building as your home)" "Somewhere else (not at your home)",
                "Somewhere else (not your home)",
            ],
        ),
        "How_often_do_you_work_elsewhere": _(
            "choice",
            items=[None, "0", "1", "2", "3", "4", "5", "6", "7", "Participant Would Not/Could Not Answer", "up to 1"],
        ),
        "How_do_you_get_to_and_from_work_school": _(
            "choice", items=[None, "Bus", "Car or Van", "On foot", "Bicycle", "Other method"]
        ),
        "Can_you_socially_distance_at_work": _(
            "choice",
            items=[
                None,
                "Difficult to maintain 2 meters - but I can usually be at least 1m from other people",
                "Easy to maintain 2m - it is not a problem to stay this far away from other people",
                "Very difficult to be more than 1 meter away as my work means I am in close contact with others on a"
                " regular basis",
            ],
        ),
        "Had_symptoms_in_the_last_7_days": _("choice", items=yes_no_none_choice),
        "Which_symptoms_in_the_last_7_days": _(
            "choice", items=[None, "Fever ", "Muscle ache", "Weakness/tiredness", "Sore  Throat"]
        ),
        "Date_of_first_symptom_onset": _(
            "discrete_distribution",
            population=[_("custom_random.random_date", start=start_date_list, end=end_date_list), None],
            weights=[0.5, 0.5],
        ),
        "Symptoms_7_Fever": _("choice", items=yes_no_none_choice),
        "Symptoms_7_Muscle_ache_myalgia": _("choice", items=yes_no_none_choice),
        "Symptoms_7_Fatigue_weakness": _("choice", items=yes_no_none_choice),
        "Symptoms_7_Sore_throat": _("choice", items=yes_no_none_choice),
        "Symptoms_7_Cough": _("choice", items=yes_no_none_choice),
        "Symptoms_7_Shortness_of_breath": _("choice", items=yes_no_none_choice),
        "Symptoms_7_Headache": _("choice", items=yes_no_none_choice),
        "Symptoms_7_Nausea_vomiting": _("choice", items=yes_no_none_choice),
        "Symptoms_7_Abdominal_pain": _("choice", items=yes_no_none_choice),
        "Symptoms_7_Diarrhoea": _("choice", items=yes_no_none_choice),
        "Symptoms_7_Loss_of_taste": _("choice", items=yes_no_none_choice),
        "Symptoms_7_Loss_of_smell": _("choice", items=yes_no_none_choice),
        "Are_you_self_Isolating_S2": _(
            "choice",
            items=[
                "No",
                "Yes because you have/have had symptoms of COVID-19 or a positive test",
                "Yes because you live with someone who has/has had symptoms or a positive test but you haven't had"
                "symptoms yourself",
                "Yes for other reasons related to reducing your risk of getting COVID-19 (e.g. going into hospital or"
                "shielding)",
                "Yes for other reasons related to you having had an increased risk of getting COVID-19 (e.g. having"
                "been in contact with a known case or quarantining after travel abroad)",
                "Participant Would Not/Could Not Answer",
                "Yes because you have/have had symptoms of COVID-19",
                "Yes because you live with someone who has/has had symptoms but you haven't had them yourself",
                "Yes for other reasons (e.g. going into hospital or quarantining)",
                None,
            ],
        ),
        "Do_you_think_you_have_Covid_Symptoms": _(
            "choice", items=["Yes", "Participant Would Not/Could Not Answer", "No", None]
        ),
        "Contact_Known_Positive_COVID19_28_days": _("choice", items=yes_no_none_choice),
        "If_Known_last_contact_date": _(
            "discrete_distribution",
            population=[_("custom_random.random_date", start=start_date_list, end=end_date_list), None],
            weights=[0.5, 0.5],
        ),
        "If_Known_type_of_contact_S2": _("choice", items=["Living in your own home", "Outside your home", None]),
        "Contact_Suspect_Positive_COVID19_28_d": _("choice", items=yes_no_none_choice),
        "If_suspect_last_contact_date": _(
            "discrete_distribution",
            population=[_("custom_random.random_date", start=start_date_list, end=end_date_list), None],
            weights=[0.5, 0.5],
        ),
        "If_suspect_type_of_contact_S2": _("choice", items=["Living in your own home", "Outside your home", None]),
        "You_been_Hospital_last_28_days": _("choice", items=yes_no_none_choice),
        "OtherHouse_been_Hospital_last_28_days": _(
            "choice", items=["Yes", "Participant Would Not/Could Not Answer", "No", None]
        ),
        "Your_been_in_Care_Home_last_28_days": _(
            "choice", items=["Yes", "Participant Would Not/Could Not Answer", "No", None]
        ),
        "OtherHouse_been_in_Care_Home_last_28_days": _(
            "choice", items=["Yes", "Participant Would Not/Could Not Answer", "No", None]
        ),
        "Hours_a_day_with_someone_else": _("choice", items=[_("random.randints", amount=8, a=0, b=24)]),
        "Physical_Contact_18yrs": _(
            "choice", items=["0", "1-5", "11-20", "21 or more", "6-10", "Participant Would Not/Could Not Answer", None]
        ),
        "Physical_Contact_18_to_69_yrs": _(
            "choice", items=["0", "1-5", "11-20", "21 or more", "6-10", "Participant Would Not/Could Not Answer", None]
        ),
        "Physical_Contact_70_yrs": _(
            "choice", items=[None, "0", "1-5", "11-20", "21 or more", "6-10", "Participant Would Not/Could Not Answer"]
        ),
        "Social_Distance_Contact_18yrs": _(
            "choice", items=[None, "0", "1-5", "11-20", "21 or more", "6-10", "Participant Would Not/Could Not Answer"]
        ),
        "Social_Distance_Contact_18_to_69_yrs": _(
            "choice", items=[None, "0", "1-5", "11-20", "21 or more", "6-10", "Participant Would Not/Could Not Answer"]
        ),
        "Social_Distance_Contact_70_yrs": _(
            "choice", items=[None, "0", "1-5", "11-20", "21 or more", "6-10", "Participant Would Not/Could Not Answer"]
        ),
        "1Hour_or_Longer_another_person_home": _(
            "choice",
            items=[
                None,
                "1",
                "2",
                "3",
                "4",
                "5",
                "6",
                "7 times or more",
                "None",
                "Participant Would Not/Could Not Answer",
            ],
        ),
        "1Hour_or_Longer_another_person_yourhome": _(
            "choice",
            items=[
                None,
                "1",
                "2",
                "3",
                "4",
                "5",
                "6",
                "7 times or more",
                "None",
                "Participant Would Not/Could Not Answer",
            ],
        ),
        "Times_Outside_Home_For_Shopping": _(
            "choice",
            items=[
                None,
                "0",
                "1",
                "2",
                "3",
                "4",
                "5",
                "6",
                "7 times or more",
                "None",
                "Participant Would Not/Could Not Answer",
            ],
        ),
        "Shopping_last_7_days": _(
            "choice",
            items=[
                None,
                "0",
                "1",
                "2",
                "3",
                "4",
                "5",
                "6",
                "7 times or more",
                "None",
                "Participant Would Not/Could Not Answer",
            ],
        ),
        "Socialise_last_7_days": _(
            "choice",
            items=[
                None,
                "0",
                "1",
                "2",
                "3",
                "4",
                "5",
                "6",
                "7 times or more",
                "None",
                "Participant Would Not/Could Not Answer",
            ],
        ),
        "Regular_testing_COVID": _("choice", items=yes_no_none_choice),
        "Face_Covering_or_Mask_outside_of_home": _(
            "choice",
            items=[
                None,
                "My face is already covered for other reasons (e.g. religious or cultural reasons)",
                "No",
                "Participant Would Not/Could Not Answer",
                "Yes at work/school only",
                "Yes in other situations only (including public transport or shops)",
                "Yes in other situations only (including public transport/shops)",
                "Yes usually both at work/school and in other situations",
            ],
        ),
        "Face_Mask_Work_Place": _(
            "choice",
            items=[
                None,
                "My face is already covered for other reasons (e.g. religious or cultural reasons)",
                "Never",
                "Not going to place of work or education",
                "Participant Would Not/Could Not Answer",
                "Yes always",
                "Yes sometimes",
            ],
        ),
        "Face_Mask_Other_Enclosed_Places": _(
            "choice",
            items=[
                None,
                "My face is already covered for other reasons (e.g. religious or cultural reasons)",
                "Never",
                "Not going to other enclosed public spaces or using public transport",
                "Participant Would Not/Could Not Answer",
                "Yes always",
                "Yes sometimes",
            ],
        ),
        "Do_you_think_you_have_had_Covid_19": _("choice", items=yes_no_none_choice),
        "think_had_covid_19_any_symptoms": _("choice", items=yes_no_none_choice),
        "think_had_covid_19_which_symptoms": _(
            "choice", items=[None, _("text.answer")]
        ),  # does this need multiple values concatted?
        "Previous_Symptoms_Fever": _("choice", items=yes_no_none_choice),
        "Previous_Symptoms_Muscle_ache_myalgia": _("choice", items=yes_no_none_choice),
        "Previous_Symptoms_Fatigue_weakness": _("choice", items=yes_no_none_choice),
        "Previous_Symptoms_Sore_throat": _("choice", items=yes_no_none_choice),
        "Previous_Symptoms_Cough": _("choice", items=yes_no_none_choice),
        "Previous_Symptoms_Shortness_of_breath": _("choice", items=yes_no_none_choice),
        "Previous_Symptoms_Headache": _("choice", items=yes_no_none_choice),
        "Previous_Symptoms_Nausea_vomiting": _("choice", items=yes_no_none_choice),
        "Previous_Symptoms_Abdominal_pain": _("choice", items=yes_no_none_choice),
        "Previous_Symptoms_Diarrhoea": _("choice", items=yes_no_none_choice),
        "Previous_Symptoms_Loss_of_taste": _("choice", items=yes_no_none_choice),
        "Previous_Symptoms_Loss_of_smell": _("choice", items=yes_no_none_choice),
        "If_yes_Date_of_first_symptoms": _(
            "discrete_distribution",
            population=[_("custom_random.random_date", start=start_date_list, end=end_date_list), None],
            weights=[0.5, 0.5],
        ),
        "Did_you_contact_NHS": _("choice", items=yes_no_none_choice),
        "Were_you_admitted_to_hospital": _("choice", items=yes_no_none_choice),
        "Have_you_had_a_swab_test": _("choice", items=yes_no_none_choice),
        "If_Yes_What_was_result": _(
            "choice",
            items=[
                "All tests failed",
                "One or more negative tests but none positive",
                "One or more negative tests but none were positive",
                "One or more positive test(s)",
                "Waiting for all results",
                None,
            ],
        ),
        "If_positive_Date_of_1st_ve_test": _(
            "discrete_distribution",
            population=[_("custom_random.random_date", start=start_date_list, end=end_date_list), None],
            weights=[0.5, 0.5],
        ),
        "If_all_negative_Date_last_test": _(
            "discrete_distribution",
            population=[_("custom_random.random_date", start=start_date_list, end=end_date_list), None],
            weights=[0.5, 0.5],
        ),
        "Have_you_had_a_blood_test_for_Covid": _("choice", items=yes_no_none_choice),
        "What_was_the_result_of_the_blood_test": _(
            "choice",
            items=[
                "All tests failed",
                "One or more negative tests but none positive",
                "One or more negative tests but none were positive",
                "One or more positive test(s)",
                "Waiting for all results",
                None,
            ],
        ),
        "Where_was_the_test_done": _(
            "choice",
            items=[
                "Home Test",
                "In the NHS (e.g. GP or hospital)",
                "Participant Would Not/Could Not Answer",
                "Private Lab",
                None,
            ],
        ),
        "If_ve_Blood_Date_of_1st_ve_test": _(
            "discrete_distribution",
            population=[_("custom_random.random_date", start=start_date_list, end=end_date_list), None],
            weights=[0.5, 0.5],
        ),
        "If_all_ve_blood_Date_last_ve_test": _(
            "discrete_distribution",
            population=[_("custom_random.random_date", start=start_date_list, end=end_date_list), None],
            weights=[0.5, 0.5],
        ),
        "Have_Long_Covid_Symptoms": _("choice", items=yes_no_none_choice),
        "Long_Covid_Reduce_Activities": _("choice", items=["Not at all", "Yes a little", "Yes a lot", None]),
        "Long_Covid_Symptoms": _(
            "choice",
            items=[
                "Fever ",
                " Headache ",
                " Muscle ache ",
                " Weakness/tiredness ",
                "Nausea/vomiting",
                "Abdominal pain",
                "Diarrhoea",
                "Sore  Throat",
                "Cough",
                "Shortness of breath",
                "Loss of taste",
                "Loss of smell",
                "Loss of appetite",
                "Chest pain",
                "Palpitations",
                "Vertigo/dizziness",
                "Worry/anxiety",
                "Low mood/not enjoying anything",
                "Trouble sleeping",
                "Memory loss or confusion",
                "Difficulty concentrating",
                "ALL No",
                "Yes",
                None,
            ],
        ),
        "Long_Covid_Fever": _("choice", items=yes_no_none_choice),
        "Long_Covid_Weakness_tiredness": _("choice", items=yes_no_none_choice),
        "Long_Covid_Diarrhoea": _("choice", items=yes_no_none_choice),
        "Long_Covid_Loss_of_smell": _("choice", items=yes_no_none_choice),
        "Long_Covid_Shortness_of_breath": _("choice", items=yes_no_none_choice),
        "Long_Covid_Vertigo_dizziness": _("choice", items=yes_no_none_choice),
        "Long_Covid_Trouble_sleeping": _("choice", items=yes_no_none_choice),
        "Long_Covid_Headache": _("choice", items=yes_no_none_choice),
        "Long_Covid_Nausea_vomiting": _("choice", items=yes_no_none_choice),
        "Long_Covid_Loss_of_appetite": _("choice", items=yes_no_none_choice),
        "Long_Covid_Sore_throat": _("choice", items=yes_no_none_choice),
        "Long_Covid_Chest_pain": _("choice", items=yes_no_none_choice),
        "Long_Covid_Worry_anxiety": _("choice", items=yes_no_none_choice),
        "Long_Covid_Memory_loss_or_confusion": _("choice", items=yes_no_none_choice),
        "Long_Covid_Muscle_ache": _("choice", items=yes_no_none_choice),
        "Long_Covid_Abdominal_pain": _("choice", items=yes_no_none_choice),
        "Long_Covid_Loss_of_taste": _("choice", items=yes_no_none_choice),
        "Long_Covid_Cough": _("choice", items=yes_no_none_choice),
        "Long_Covid_Palpitations": _("choice", items=yes_no_none_choice),
        "Long_Covid_Low_mood_not_enjoying_anything": _("choice", items=yes_no_none_choice),
        "Long_Covid_Difficulty_concentrating": _("choice", items=yes_no_none_choice),
        "Have_you_been_offered_a_vaccination": _("choice", items=yes_no_none_choice),
        "Vaccinated_Against_Covid": _("choice", items=yes_no_none_choice),
        "Type_Of_Vaccination": _(
            "choice",
            items=[
                "Don't know type",
                "From a research study/trial",
                "Janssen/Johnson&Johnson",
                "Moderna",
                "Novavax",
                "Other / specify",
                "Oxford/AstraZeneca",
                "Pfizer/BioNTech",
                "Sinopharm",
                "Sinovac",
                "Sputnik",
                "Valneva",
                None,
            ],
        ),
        "Vaccination_Other": _(
            "choice", items=["OtherVaxx", None]
        ),  # This is usually freetext, depends on the previous question, may need to change
        "Number_Of_Doses": _("choice", items=["1", "2", "3 or more", None]),
        "Date_Of_Vaccination": _(
            "discrete_distribution",
            population=[_("custom_random.random_date", start=start_date_list, end=end_date_list), None],
            weights=[0.5, 0.5],
        ),
        "Have_you_been_outside_UK_since_April": _("choice", items=yes_no_none_choice),
        "been_outside_uk_last_country": _(
            "choice",
            items=[
                "Afghanistan",
                "Bangladesh",
                "Cambodia",
                "Denmark",
                "Ecuador",
                "Gabon",
                "Honduras",
                "Iceland",
                "Jamaica",
                "Kenya",
                "Latvia",
                "Madagascar",
                "Namibia",
                "Oman",
                "Pakistan",
                "Qatar",
                "Romania",
                "Samoa",
                "Thailand",
                "Uganda",
                "Venezuela",
                "Zimbabwe",
                None,
            ],
        ),
        "been_outside_uk_last_date": _(
            "discrete_distribution",
            population=[_("datetime.formatted_datetime", fmt="%d/%m/%Y", start=2020, end=2022), None],
            weights=[0.5, 0.5],
        ),
        "Have_you_been_outside_UK_Lastspoke": _("choice", items=yes_no_none_choice),
    }
