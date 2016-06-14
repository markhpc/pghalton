#!/bin/python

#import settings
from itertools import count

class Bucket_Layout:

    def __init__(self, bucket_count):
        self.set_bucket_count(bucket_count)

    def halton(self, i, prime):
        h = 0.0
        f = 1.0/prime
        fct = 1.0
        while (i>0):
            fct *= f
            h += (i%prime)*fct
            i /= prime
        return h

    def set_bucket_count(self, bucket_count):
        self.bucket_count = bucket_count
        self.layout = self.get_layout()

    def order(self):
        return self.compute_order(self.bucket_count)

    @staticmethod
    def compute_order(value):
       order = 0
       while 2**order < value:
           order +=1
       return order

    def print_layout(self):
         print self.layout

    def get_bucket(self, index, bucket_count, prime):
        return self.layout[int(self.halton(index, prime)*bucket_count)]
    # Use the A003602 fractal sequence for creating the bucket layout
    # This is done to so that buckets remain positioned at the same place in
    # the halton sequence so that data movement is minimal when splitting
    # buckets.
    # ie with 2 buckets (order 1) we have 1,2 with halton ranges 0-0.5 going
    # to bucket 1 and 0.5-1 going to bucket 2.
    #
    # Now we switch to order 2, resulting in a new sequence 1,3,2,4 with
    # 1 -> 0.00 - 0.25
    # 3 -> 0.25 - 0.50
    # 2 -> 0.50 - 0.75
    # 4 -> 0.75 - 1.00
    def A003602(self):
        x=count(1)
        y=self.A003602()
        while True:
           yield next(x)
           yield next(y)

    # Get the bucket layout given a certain order of buckets.  This should
    # follow the fractal sequence as described above starting at a given
    # order minus 1, ie:
    #    1 bucket order 0, [0]
    #    2 buckets order 1, [0,1]
    #    4 buckets order 2, [0,2,1,3]
    #    8 buckets order 3, [0,4,2,5,1,6,3,7]

    def get_layout(self):
        gen = self.A003602()
        start = length = 2**self.order()
        ret = []
        for i in range(1, start+length):
            val = next(gen)
            if i >= length:
                ret.append(val-1)
        return ret

