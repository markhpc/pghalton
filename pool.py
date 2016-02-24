#!/bin/python

import settings
from bucket_layout import Bucket_Layout
from pg import PG

class Pool:
    def __init__(self):
        gs = settings.general
        self.replication = gs.get('replication', 0)
        self.pgs = gs.get('pgs', 0)
        self.osds = gs.get('osds', 0)

        order = Bucket_Layout.compute_order(self.osds)
        self.bl = Bucket_Layout(2**order)

        self.offset = gs.get('offset', 0)
        self.pg_list = []
        
        for i in xrange(0, self.pgs):
            pg = PG(i, self)
            self.pg_list.append(pg)
        
        self.remap_acting()
        self.remap_up()

    def remap_acting(self):
        self.bl.set_bucket_count(2**self.bl.compute_order(self.osds))

        for i in xrange(0, self.pgs):
            pg = self.pg_list[i]
            pg.remap_acting()

    def remap_up(self):
        self.make_up_map()
#        print "up_map: %s" % self.up_map
        up_index = 0
        for i in xrange(0, self.pgs):
            pg = self.pg_list[i]
#            print "before pool up remap, up_index: %s" % up_index
            up_index = pg.remap_up(self.up_map, up_index)
#            print "after pool up remap, up_index: %s" % up_index

    def set_osds(self, osds):
        cur = self.get_all_up()
        self.osds = osds
#        print "osds: %s" % self.osds
        self.remap_acting()
        self.remap_up()
        n = self.get_all_up()

        count = 0
        for i in xrange(0, len(n)):
            if cur[i] != n[i]:
                count += 1
        print "Remapped placements: %s" % count

           
    def get_all_up(self):
        l = []
        for pg in self.pg_list:
            l += pg.get_up()
        return l

    def make_up_map(self):
        down_osds = []
        for d in xrange(self.osds, 2**self.bl.order()):
           down_osds.append(d)
        
        up_map = []
        for osd in xrange(0, 2**self.bl.order()):
            if osd not in down_osds:
                up_map.append(osd)
        self.up_map = up_map

    def print_counts(self):
        acting_counts = [0]*(2**self.bl.order())
        up_counts = [0]*(2**self.bl.order())

        for pg in self.pg_list:
            for osd in pg.get_acting():
                acting_counts[osd] += 1
            for osd in pg.get_up():
                up_counts[osd] += 1

        print "acting counts:"
        print acting_counts
        print "up_counts"
        print up_counts
