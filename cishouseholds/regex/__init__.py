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
