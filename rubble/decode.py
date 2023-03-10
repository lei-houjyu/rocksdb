#!/usr/bin/python

import sys

s = sys.argv[1]

if len(s) % 2 != 0:
    print("input string has odd number of chars")
    exit(1)

res = ''
for i in range(len(s) // 2):
    h = s[i * 2 : i * 2 + 2]
    res += chr(int(h, 16))
    
print(res)