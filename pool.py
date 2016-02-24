#!/bin/python

import settings
from pg import PG

class Pool:
    def __init__(self):
        gs = settings.general
        self.replication = gs.get('replication', 0)
        self.pgs = gs.get('pgs', 0)
        self.potential_osds = gs.get('potential_osds', 0)
        self.acting_osds = gs.get('acting_osds', 0)
        self.offset = gs.get('offset', 0)
        self.pg_list = []

        down_osds = []
        for d in xrange(self.acting_osds, self.potential_osds):
           down_osds.append(d)

        self.up_map = self.make_up_map(down_osds)
        
        up_index = 0
        for i in xrange(0, self.pgs):
            pg = PG(i, self)
            up_index = pg.remap_acting(self.up_map, up_index)
            up_index = pg.remap_up(self.up_map, up_index)
            self.pg_list.append(pg)

    def remap_acting(self):
        up_index = 0
        for i in xrange(0, self.pgs):
            pg = self.pg_list[i]
            up_index = pg.remap_acting(self.up_map, up_index)

    def remap_up(self):
        up_index = 0
        for i in xrange(0, self.pgs):
            pg = self.pg_list[i]
#            print "before pool up remap, up_index: %s" % up_index
            up_index = pg.remap_up(self.up_map, up_index)
#            print "after pool up remap, up_index: %s" % up_index
    def set_potential_osds(self, potential_osds):
        cur = self.get_all_up()
        self.potential_osds = potential_osds
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

    def make_up_map(self, down_map):
        up_map = []
        for osd in xrange(0, self.potential_osds):
            if osd not in down_map:
                up_map.append(osd)
        return up_map

    def print_counts(self):
        acting_counts = [0]*self.potential_osds
        up_counts = [0]*self.potential_osds

        for pg in self.pg_list:
            for osd in pg.get_acting():
                acting_counts[osd] += 1
            for osd in pg.get_up():
                up_counts[osd] += 1

        print "acting counts:"
        print acting_counts
        print "up_counts"
        print up_counts

