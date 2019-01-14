#!/usr/local/bin/python3

import sys
import random

if __name__ == '__main__':
    print(sys.argv, file=sys.stderr)
    print(random.randint(1, 1000000))
    if random.randint(0,1):
    	exit(1)# failed ingest
