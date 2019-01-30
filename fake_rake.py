#!/usr/local/bin/python3
import time
import sys
import random

if __name__ == '__main__':
    print(sys.argv, file=sys.stderr)
    print(random.randint(1, 1000000))
    time.sleep(1)
    with open('batch-loader/ingest_status.log','r') as f:
    	lists = []
    	list2 = []
    	for i in f.readlines():
    		lists.append(len(i))
    		list2.append(max([i ** 0.5 + random.randint(-4,4) for i in lists])) #i just want this to be slow
    print(sum(lists),max(list2))
    # if random.randint(0,1):
    # 	exit(1)# failed ingest
