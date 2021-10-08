import datetime
import random as random_module
from typing import Any
from typing import List

from mimesis.providers.base import BaseDataProvider


def coded_num_in_range(min, max):
    value = random_module.randint(int(min), int(max))
    if len(str(value)) < len(str(max)):
        str_val = "0" * (len(max) - len(str(value))) + str(value)
    else:
        str_val = str(value)

    return str_val


def get_breakpoints(mask):
    key_chars = ["#", "&"]
    dic = {"#": [], "&": []}
    pos = []
    for i, char in enumerate(mask):
        add = False
        if char in key_chars:
            if i == 0 or (i > 0 and mask[i - 1] != char):
                pos.append(i)
            try:
                if mask[i + 1] != char:
                    pos.append(i)
                    add = True
            except IndexError:
                pos.append(i)
                add = True
            if add:
                try:
                    dic[char].append(pos)
                    pos = []
                except KeyError as e:
                    print(e)
    return dic


def get_nums(number_positions, string):
    numbers = []
    for pos_range in number_positions:
        string_num = ""
        for pos in range(pos_range[0], pos_range[1] + 1):
            string_num += string[pos]
        numbers.append(string_num)
    return numbers


def coded_string_in_range(length, use_incremental_letters, min_string="", max_string=""):
    if use_incremental_letters:
        min_char = min_string[0]
        max_char = max_string[0]
    else:
        min_char = "A"
        max_char = "Z"

    string = ""
    for i in range(0, length):
        if i > 0 and use_incremental_letters:
            min_char = "A"
            if string[i - 1] == max_char:
                max_char = max_string[i]
            else:
                max_char = "Z"

        string += chr(random_module.randint(ord(min_char), ord(max_char)))
    return string


def pick_specific_character(mask):
    while "[" in mask and "]" in mask:
        new_flag_indexes = [mask.index("["), mask.index("]")]
        sub_string = ""
        for c in range(new_flag_indexes[0] + 1, new_flag_indexes[1]):
            sub_string += mask[c]
        character_choices = sub_string.split(",")
        mask = (
            mask[: new_flag_indexes[0]]
            + random_module.choice(character_choices)
            + mask[new_flag_indexes[1] + 1 :]  # noqa: E203
        )

    return mask


def replace_static_characters(mask, min_code):
    for i, c in enumerate(mask):
        if c == "X":
            mask = mask[:i] + min_code[i] + mask[i + 1 :]  # noqa: E203
        return mask


def code_mask(**kwargs):
    try:
        mask = kwargs["mask"]
    except KeyError:
        raise Exception("mask must be provided")

    try:
        weights = kwargs["weights"]
    except KeyError:
        weights = None

    if "[" in mask and "]" in mask:
        mask = pick_specific_character(mask)

    if isinstance(kwargs["min_code"], List):
        if isinstance(kwargs["max_code"], List) and (len(kwargs["max_code"]) == len(kwargs["min_code"])):
            randindex = kwargs["min_code"].index(random_module.choices(kwargs["min_code"], weights=weights, k=1)[0])
            min_code = kwargs["min_code"][randindex]
            max_code = kwargs["max_code"][randindex]
            if min_code is None:
                return None
            mask = replace_static_characters(mask, min_code)
        else:
            raise Exception("ranged lists (min_code, max_code) must be same length")
    else:
        min_code = kwargs["min_code"]
        max_code = kwargs["max_code"]
    try:
        use_incremental_letters = kwargs["use_incremental_letters"]
    except KeyError:
        use_incremental_letters = False

    breakpoints = get_breakpoints(mask)
    min_nums = get_nums(breakpoints["#"], min_code)  # numbers are strings --> convert to int
    max_nums = get_nums(breakpoints["#"], max_code)
    for pos_range in breakpoints["&"]:
        try:
            stop = pos_range[1] + 1
        except IndexError:
            stop = pos_range[0] + 1
        start = pos_range[0]
        coded_string = coded_string_in_range(
            stop - start, use_incremental_letters, min_code[start:stop], max_code[start:stop]
        )
        for i, j in enumerate(range(start, stop)):
            mask = mask[:j] + coded_string[i] + mask[j + 1 :]  # noqa: E203

    for i, pos_range in enumerate(breakpoints["#"]):
        try:
            stop = pos_range[1] + 1
        except IndexError:
            stop = pos_range[0] + 1
        start = pos_range[0]
        coded_num = coded_num_in_range(min_nums[i], max_nums[i])
        for j, k in enumerate(range(start, stop)):
            mask = mask[:k] + coded_num[j] + mask[k + 1 :]  # noqa: E203

    return mask


class CustomRandom(BaseDataProvider):
    """
    Class for generating random numbers and dates.
    """

    class Meta:
        name = "custom_random"

    def __init__(self, *args: Any, **kwargs: Any) -> None:

        super().__init__(*args, **kwargs)

    def random_date(self, start, end, format="%d/%m/%Y"):
        """Generate a random_module datetime between datetime object `start` and `end`"""
        return (
            start
            + datetime.timedelta(
                # Get a random_module amount of seconds between `start` and `end`
                seconds=int(self.random.random() * (end - start).total_seconds()),
            )
        ).strftime(format)

    def random_integer(self, lower: int, upper: int, null_percent: int = -1):
        choice = self.random.random()
        if null_percent <= choice:
            return str(int(self.random.random() * (upper - lower)))
        else:
            return None
