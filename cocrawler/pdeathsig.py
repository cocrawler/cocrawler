'''
The python-prctl package supports PDEATHSIG, but you have to install libcap-dev
to install it. This is a minimal workaround.
'''

import sys
import ctypes.util

libc = ctypes.CDLL(ctypes.util.find_library('c'))
PR_SET_DEATHSIG=1

def set_pdeathsig(sig):
    libc.prctl(PR_SET_DEATHSIG, sig, 0, 0, 0)
