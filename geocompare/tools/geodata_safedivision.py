from geocompare.tools.numeric import safe_divide


def gdsd(dividend, divisor, verbose=False, divbyzero=0):
    return safe_divide(dividend, divisor, divide_by_zero=divbyzero)


__all__ = ["gdsd", "safe_divide"]
