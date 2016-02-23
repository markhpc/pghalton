#!/bin/python

from itertools import count

PRIMES = [2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97]

def halton(i, prime):
    h = 0.0
    f = 1.0/prime
    fct = 1.0
    while (i>0):
        fct *= f
        h += (i%prime)*fct
        i /= prime
    return h

def get_bucket(index, bucket_count, prime):
    return int(halton(index, prime)*bucket_count)

# Use a fractal sequence for creating the bucket layout
# This is done to so that buckets remain positioned at the same place in
# the halton sequence so that data movement is minimal when splitting buckets.
# ie with 2 buckets (order 1) we have 1,2 with halton ranges 0-0.5 going to
# bucket 1 and 0.5-1 going to bucket 2.
#
# Now we switch to order 2, resulting in a new sequence 1,3,2,4 with
# 1 -> 0.00 - 0.25
# 3 -> 0.25 - 0.50
# 2 -> 0.50 - 0.75
# 4 -> 0.75 - 1.00

def A003602():
    x=count(1)
    y=A003602()
    while True:
        yield next(x)
        yield next(y)

# Get the bucket layout given a certain order of buckets
# ie 1 bucket order 0
#    2 buckets order 1
#    4 buckets order 2
#    8 buckets order 3 ...

def get_layout(order):
    gen = A003602()
    start = length = 2**order
    ret = []
    for i in range(1, start+length):
        val = next(gen)
        if i >= length:
            ret.append(val)
    return ret

