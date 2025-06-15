import math
import os
import random
import re
import sys

def solve(s):
    r = ''
    i = 0
    n = len(s)
    while i < n:
        if s[i].isalpha():
            if i == 0 or s[i-1] == ' ':
                r += s[i].upper()
            else:
                r += s[i]
        else:
            r += s[i]
        i += 1
    return r
if __name__ == '__main__':
    fptr = open(os.environ['OUTPUT_PATH'], 'w')
    s = input()
    result = solve(s)
    fptr.write(result + '\n')
    fptr.close()