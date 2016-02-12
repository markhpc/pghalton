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

        self.acting_primary = get_bucket(offset+(index*rep), potential_osds)
        self.acting_secondaries = []
        for r in xrange(1, rep):
            self.acting_secondaries.append(get_bucket(offset+(index*rep)+r, potential_osds))

        self.up_primary = self.acting_primary
        self.up_secondaries = list(self.acting_secondaries)

    def using_osd(self, osd):
       if self.acting_primary == osd:
          return True
       if osd in self.acting_secondaries:
          return True

    def remap(self, up_map, up_index):
       remap_count = 0
       # revert to the acting_primary if it's back up
       if self.up_primary != self.acting_primary:
           if self.acting_primary in up_map:
               self.up_primary = self.acting_primary
               remap_count +=1
       # If the up_primary is no longer up, find a new one
       if self.up_primary not in up_map:
          self.up_primary = up_map[get_bucket(up_index, len(up_map))]
          up_index+=1
          remap_count+=1

       for i in xrange(0, len(self.up_secondaries)):
          # rever to the acting_secondary if it's back up
          if self.up_secondaries[i] != self.acting_secondaries[i]:
              if self.acting_secondaries[i] in up_map:
                  self.up_secondaries[i] = self.acting_secondaries[i]
                  remap_count+=1
          # If the up_secondary is no longer up, find a new one
          if self.up_secondaries[i] not in up_map:
              self.up_secondaries[i] = up_map[get_bucket(up_index, len(up_map))]
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
       ret = []
       ret.append(self.acting_primary)
       for osd in self.acting_secondaries:
           ret.append(osd)
       return ret

    def get_up(self):
       ret = []
       ret.append(self.up_primary)
       for osd in self.up_secondaries:
           ret.append(osd)
       return ret

