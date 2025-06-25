import sys
import platform
import traceback
import threading
import os

from time import sleep
from datetime import datetime, timedelta

from .constants import DATETIME_FORMATS


class GlobalInfo:
    gmt_timedelta = timedelta(hours=0)
    gmt_offset = -180


def str_truncate(s, max_len):
    if type(s) is str and len(s) > max_len:
        return f"{s[:max_len-5]}...[{len(s)}]"
    else:
        return s


def remove_accents_from_string(s):
    chars_table = {}

    if not s:
        return ''

    if not isinstance(s, str):
        return ''

    chars_table = {
        192: 'A', 193: 'A', 194: 'A', 195: 'A', 196: 'A', 197: 'A', 199: 'C', 200: 'E', 201: 'E', 202: 'E',
        203: 'E', 204: 'I', 205: 'I', 206: 'I', 207: 'I', 210: 'O', 211: 'O', 212: 'O', 213: 'O', 214: 'O',
        217: 'U', 218: 'U', 219: 'U', 220: 'U', 224: 'a', 225: 'a', 226: 'a', 227: 'a', 228: 'a', 229: 'a',
        231: 'c', 232: 'e', 233: 'e', 234: 'e', 235: 'e', 236: 'i', 237: 'i', 238: 'i', 239: 'i', 240: 'o',
        241: 'n', 242: 'o', 243: 'o', 244: 'o', 245: 'o', 246: 'o', 249: 'u', 250: 'u', 251: 'u', 252: 'u',
        253: 'y', 255: 'y', 160: ' ',
    }

    e = ''
    for c in s:
        if ord(c) <= 128:
            e += c
        else:
            e += chars_table.get(ord(c), '_')
    return e


# Datetime

def get_localtime():
    if is_windows():
        return datetime.utcnow() + GlobalInfo.gmt_timedelta
    else:
        return datetime.today()


def get_utctime():
    if is_windows():
        return datetime.utcnow()
    else:
        return datetime.today() - GlobalInfo.gmt_timedelta


def format_date(x: datetime) -> str:
    if not x:
        return "-"
    return "%04d-%02d-%02d %02d:%02d:%02d.%03d" % (x.year, x.month, x.day, x.hour, x.minute, x.second, x.microsecond / 1000)


def parse_date(date_str: str) -> datetime | None:
    """Tenta converter a string de data usando vários formatos."""
    if not date_str:
        return None

    for fmt in DATETIME_FORMATS:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None


def is_windows():
    return True if platform.system() == "Windows" else False


def is_linux():
    return True if platform.system() == "Linux" else False


def check_os():
    os_type = platform.system()
    if os_type == "Windows":
        return "Windows"
    elif os_type == "Linux":
        return "Linux"
    elif os_type == "Darwin":
        return "MacOS"
    else:
        return f"Unknown: {os_type}"
