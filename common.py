#!/bin/python

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
