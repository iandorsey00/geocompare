try:
    from geodata.tools.geodata_typecast import gdt, gdti, gdtf
except ImportError:  # pragma: no cover - script execution fallback
    from tools.geodata_typecast import gdt, gdti, gdtf
import numpy as np

def gdsd(dividend, divisor, verbose=False, divbyzero=0):
    '''Error-tolerant division to suit the needs of geodata.'''
    dividend = gdt(dividend, verbose=verbose)
    # divisor will always be converted to a float.
    divisor = gdtf(divisor, verbose=verbose)

    # return divbyzero if the divisor is zero.
    if divisor == 0.0:
        if verbose:
            print('gdsd: Division by zero. Returning {}'.format(divbyzero))
        return divbyzero
    # If either arguments are numpy.nan, return numpy.nan
    elif dividend == np.nan or divisor == np.nan:
        if verbose:
            print('gdsd: numpy.nan. Returning numpy.nan')
        return np.nan
    # Else, return the quotient.
    else:
        return dividend / divisor
