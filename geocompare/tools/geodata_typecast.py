import numpy

from geocompare.tools.numeric import parse_float, parse_int, parse_number

def gdt(input_val, dtype='default', verbose=False, not_num=numpy.nan, allow_negs=True):
    '''Aggressive, error-tolerant typecasting to clean up dirty data.'''
    return parse_number(
        input_val,
        as_type=dtype,
        default=not_num,
        allow_negative=allow_negs,
    )

def gdti(input_val, verbose=False, not_num=numpy.nan, allow_negs=True):
    '''A wrapper function that only returns ints.'''
    return parse_int(
        input_val,
        default=not_num,
        allow_negative=allow_negs,
    )

def gdtf(input_val, verbose=False, not_num=numpy.nan, allow_negs=True):
    '''A wrapper function that only returns floats.'''
    return parse_float(
        input_val,
        default=not_num,
        allow_negative=allow_negs,
    )
