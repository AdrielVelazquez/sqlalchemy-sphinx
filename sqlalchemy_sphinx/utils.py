import re


SPECIAL_CHARS_RE = re.compile("([@()])")


def escape_percent_char(match_string):
    if match_string:
        if "%" in match_string:
            match_string = match_string.replace("%", "%%")
    return match_string


def escape_special_chars(match_string):
    if match_string:
        match_string = escape_percent_char(match_string)
        if SPECIAL_CHARS_RE.search(match_string):
            match_string = SPECIAL_CHARS_RE.sub(r"\\\\\1", match_string)
    return match_string
