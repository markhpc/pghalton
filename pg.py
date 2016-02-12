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
            self.acting.append(get_bucket(offset+(index*rep)+r, potential_osds, 2))

        self.up = list(self.acting)

    def using_osd(self, osd):
       if osd in self.acting:
          return True

    def remap(self, up_map, up_index):
       remap_count = 0

       for i in xrange(0, len(self.up)):
          # if the upset and acting set don't match, we might have to rebalance 
          if self.up[i] != self.acting[i]:
              # revert to the acting OSD if it's back up
              if self.acting[i] in up_map:
                  print "acting OSD is back, reverting up: %s to acting: %s" % (self.up[i], self.acting[i])
                  self.up[i] = self.acting[i]
                  remap_count+=1
              # otherwise, try recomputing
              else:
                  up_index, new = self.get_new_up(up_map, up_index)
                  # If we didn't get the same OSD as last time, there's a new map and we have to rebalance
                  if self.up[i] != new:
                      self.up[i] = new
                      remap_count+=1

          # If the up OSD is no longer up, find a new one
          if self.up[i] not in up_map:
              up_index, self.up[i] = self.get_new_up(up_map, up_index)
              remap_count+=1
       PG.REMAP_COUNT+=remap_count
       return up_index

    def get_new_up(self, up_map, up_index):
          # Use the next prime for the remap sequence to help avoid OSD collisions
          new = up_map[get_bucket(up_index, len(up_map), 3)]
          up_index+=1
          # Make sure the new OSD isn't already in the up map. If so move to next index.
          # So long as the ordering is the same, this should be repeatable.
          while new in self.up:
              new = up_map[get_bucket(up_index, len(up_map), 3)]
              up_index+=1
          return up_index, new

    @staticmethod
    def reset_remap_counter():
       PG.REMAP_COUNT = 0
    @staticmethod
    def print_remap_counter():
       print "Total OSD PG Targets remapped: %d" % PG.REMAP_COUNT

    def print_osds(self):
       print "pg: %s, acting: %s, up: %s" % (self.index, self.acting, self.up)

    def get_acting(self):
        return self.acting

    def get_up(self):
        return self.up

