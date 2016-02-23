#!/bin/python

import settings
from bucket_layout import Bucket_Layout
from common import *

class PG:
    REMAP_COUNT = 0
    def __init__(self, pg_id):
        self.pg_id = pg_id
        gs = settings.general
        rep = gs.get('replication', 0)
        potential_osds = gs.get('potential_osds', 0)
        self.bl = Bucket_Layout(potential_osds)

        offset = gs.get('offset', 0)

        self.acting = []
        self.up = []
        for r in xrange(0, rep):
            new = self.bl.get_bucket(offset+(pg_id*rep)+r, potential_osds, 2)
            self.acting.append(new)
            self.up.append(new)

    def using_osd(self, osd):
       if osd in self.acting:
          return True

    def remap(self, up_map, up_index):
       remap_count = 0

       for r in xrange(0, len(self.up)):
          # if the upset and acting set don't match, we might have to rebalance 
          if self.up[r] != self.acting[r]:
              # revert to the acting OSD if it's back up
              if self.acting[r] in up_map:
                  print "acting OSD is back, reverting up: %s to acting: %s" % (self.up[r], self.acting[r])
                  self.up[r] = self.acting[r]
                  remap_count+=1
              # otherwise, try recomputing
              else:
                 # Use the next prime for the remap sequence to help avoid OSD collisions
                  up_index, new = self.get_new_up(up_map, up_index, 3)
                  # If we didn't get the same OSD as last time, there's a new map and we have to rebalance
                  if self.up[r] != new:
                      self.up[r] = new
                      remap_count+=1

          # If the up OSD is no longer up, find a new one
          if self.up[r] not in up_map:
              # Use the next prime for the remap sequence to help avoid OSD collisions
              up_index, self.up[r] = self.get_new_up(up_map, up_index, 3)
              remap_count+=1
       PG.REMAP_COUNT+=remap_count
       return up_index

    def get_new_up(self, up_map, index, prime):
          new = up_map[self.bl.get_bucket(index, len(up_map), prime)]
          index+=1
          # Make sure the new OSD isn't already in the up map. If so move to next index.
          # So long as the ordering is the same, this should be repeatable.
          while new in self.up:
              new = up_map[self.bl.get_bucket(index, len(up_map), prime)]
              index+=1
          return index, new

#    def get_osd(self, up_map

    @staticmethod
    def reset_remap_counter():
       PG.REMAP_COUNT = 0

    @staticmethod
    def print_remap_counter():
       gs = settings.general
       percent = float(100 * PG.REMAP_COUNT) / (gs.get('pgs', 0) * gs.get('replication', 0))
       print "OSD PG Targets remapped: %d, %.1f%% of total." % (PG.REMAP_COUNT, percent) 

    def print_osds(self):
       print "pg: %s, acting: %s, up: %s" % (self.pg_id, self.acting, self.up)

    def get_acting(self):
        return self.acting

    def get_up(self):
        return self.up

