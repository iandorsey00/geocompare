try:
    from geocompare.tools.numeric import safe_divide
except ImportError:  # pragma: no cover - script execution fallback
    try:
        from geodata.tools.numeric import safe_divide
    except ImportError:  # pragma: no cover - script execution fallback
        from tools.numeric import safe_divide

def gdsd(dividend, divisor, verbose=False, divbyzero=0):
    '''Error-tolerant division to suit the needs of geodata.'''
    return safe_divide(dividend, divisor, divide_by_zero=divbyzero)
