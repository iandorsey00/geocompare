import math
from typing import Any

import numpy as np


def parse_number(
    value: Any,
    *,
    as_type: str = "default",
    default: float = np.nan,
    allow_negative: bool = True,
) -> Any:
    """Parse dirty numeric inputs into int/float with tolerant cleanup."""
    if isinstance(value, (int, float)):
        return value

    if not isinstance(value, str) or value == "":
        return default

    negative = value.startswith("-")
    filtered = "".join(ch for ch in value if ch.isdigit() or ch == ".")

    if "." in filtered:
        parts = filtered.split(".")
        filtered = parts[0] + "." + "".join(parts[1:]).replace(".", "")

    if filtered and set(filtered) == {"."}:
        filtered = ""

    if not filtered:
        return default

    if negative and allow_negative:
        filtered = "-" + filtered

    if as_type == "int":
        return int(filtered.split(".")[0] or 0)
    if as_type == "float":
        return float(filtered)
    if as_type == "default":
        return float(filtered) if "." in value else int(filtered)

    raise ValueError(f"Unsupported as_type: {as_type}")


def parse_int(
    value: Any,
    *,
    default: float = np.nan,
    allow_negative: bool = True,
) -> Any:
    return parse_number(
        value,
        as_type="int",
        default=default,
        allow_negative=allow_negative,
    )


def parse_float(
    value: Any,
    *,
    default: float = np.nan,
    allow_negative: bool = True,
) -> Any:
    return parse_number(
        value,
        as_type="float",
        default=default,
        allow_negative=allow_negative,
    )


def safe_divide(
    dividend: Any,
    divisor: Any,
    *,
    divide_by_zero: float = 0,
    default: float = np.nan,
) -> float:
    left = parse_number(dividend, default=default)
    right = parse_float(divisor, default=default)

    if isinstance(left, float) and math.isnan(left):
        return np.nan
    if isinstance(right, float) and math.isnan(right):
        return np.nan
    if right == 0.0:
        return divide_by_zero
    return left / right
