#!/bin/python
from decimal import Decimal

REPLICATION = 3
POTENTIAL_OSDS = 1000 
ACTING_OSDS = 50
VIRTUAL_PGS = 10000
OFFSET = 351
OFFSET2 = 1341
DOWN_OSDS = [4, 10, 31]

def halton(i, prime):
    h = Decimal(0.0)
    f = Decimal(1.0/prime)
    fct = Decimal(1.0)
    while (i>0):
        fct *= f
        h += (i%prime)*fct
        i /= prime
    return Decimal(h)

#def find_next(pglist):
    

#def class pg:
#    acting = -1
#    secondary = [-1]*(REPLICATION-1)

PRIMES = [2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97]


class PG:
    REMAP_COUNT = 0
    def __init__(self, index):
        self.index = index

        self.acting_primary = get_osd(OFFSET+(index*REPLICATION), POTENTIAL_OSDS) 
        self.acting_secondaries = []
        for r in xrange(1, REPLICATION):
            self.acting_secondaries.append(get_osd(OFFSET+(index*REPLICATION)+r, POTENTIAL_OSDS))

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
          self.up_primary = up_map[get_osd(up_index, len(up_map))]
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
              self.up_secondaries[i] = up_map[get_osd(up_index, len(up_map))] 
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

def make_up_map():
    up_map = []
    for osd in xrange(0, POTENTIAL_OSDS):
        if osd not in DOWN_OSDS:
            up_map.append(osd)
    return up_map
 
def get_osd(index, osd_count):
    return int(halton(index, PRIMES[0])*osd_count)

if __name__=='__main__':
    for d in xrange(ACTING_OSDS, POTENTIAL_OSDS):
       DOWN_OSDS.append(d)

    up_map = make_up_map()
    print up_map
    up_index = 0
    pg_list = []

    PG.reset_remap_counter() 
    for i in xrange(0, VIRTUAL_PGS):
        pg = PG(i)
        pg_list.append(pg)
#        pg.print_osds()
        up_index = pg.remap(up_map, up_index)
#        pg.print_osds()
    PG.print_remap_counter()

    acting_counts = [0]*(POTENTIAL_OSDS)
    up_counts = [0]*(POTENTIAL_OSDS)

    for pg in pg_list:
        for osd in pg.get_acting():
            acting_counts[osd]+=1
        for osd in pg.get_up():
            up_counts[osd]+=1

    print acting_counts
    print up_counts
