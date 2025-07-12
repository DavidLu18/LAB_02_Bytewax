


def to_float(v, default=0):
    try:
        return float(v)
    except (ValueError, TypeError):
        return default

def to_int(v, default=0):
    try:
        return int(v)
    except (ValueError, TypeError):
        return default

def to_str(v, default=""):
    try:
        return str(v)
    except (ValueError, TypeError):
        return default
