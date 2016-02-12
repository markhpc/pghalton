#!/bin/python

import settings
from common import *

class PG:
    REMAP_COUNT = 0
    def __init__(self, index):
        self.index = index
        gs = settings.general
        rep = gs.get('replication', 0)
        potential_osds = gs.get('potential_osds', 0)
        offset = gs.get('offset', 0)

        self.acting = []
        for r in xrange(0, rep):
            self.acting.append(get_bucket(offset+(index*rep)+r, potential_osds))

        self.up = list(self.acting)

    def using_osd(self, osd):
       if osd in self.acting:
          return True

    def remap(self, up_map, up_index):
       remap_count = 0

       for i in xrange(0, len(self.up)):
          # rever to the acting OSD if it's back up
          if self.up[i] != self.acting[i]:
              if self.acting[i] in up_map:
                  self.up[i] = self.acting[i]
                  remap_count+=1
          # If the up OSD is no longer up, find a new one
          if self.up[i] not in up_map:
              self.up[i] = up_map[get_bucket(up_index, len(up_map))]
              up_index+=1
              remap_count+=1
       PG.REMAP_COUNT+=remap_count
       return up_index

    @staticmethod
    def reset_remap_counter():
       PG.REMAP_COUNT = 0
    @staticmethod
    def print_remap_counter():
       print "Total OSD PG Targets remapped: %d" % PG.REMAP_COUNT


    def print_osds(self):
       print "pg: %s, acting: %s, up: %s" % (self.index, (self.acting_primary, self.acting_secondaries), (self.up_primary, self.up_secondaries))

    def get_acting(self):
        return self.acting

    def get_up(self):
        return self.up

