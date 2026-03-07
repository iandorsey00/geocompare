from geocompare.tools.numeric import safe_divide

def gdsd(dividend, divisor, verbose=False, divbyzero=0):
    '''Error-tolerant division to suit the needs of geodata.'''
    return safe_divide(dividend, divisor, divide_by_zero=divbyzero)
