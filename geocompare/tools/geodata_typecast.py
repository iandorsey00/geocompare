from geocompare.tools.numeric import parse_float, parse_int, parse_number


def gdt(input_val, dtype="default", verbose=False, not_num=float("nan"), allow_negs=True):
    return parse_number(
        input_val,
        as_type=dtype,
        default=not_num,
        allow_negative=allow_negs,
    )


def gdti(input_val, verbose=False, not_num=float("nan"), allow_negs=True):
    return parse_int(
        input_val,
        default=not_num,
        allow_negative=allow_negs,
    )


def gdtf(input_val, verbose=False, not_num=float("nan"), allow_negs=True):
    return parse_float(
        input_val,
        default=not_num,
        allow_negative=allow_negs,
    )


__all__ = ["gdt", "gdti", "gdtf", "parse_number", "parse_int", "parse_float"]
