
# the purpose of this file is to prove that the algorithm we use for the
# property_crime is correct. But it takes very long time to process the huge data.
# Professor Lapets told us that we will learn a better method to compare the distance.

property_2014 = [(100,(1,1)),(200,(5,5)),(150,(3,5))]

crime = [(0.5,1),(8,5)]






def union(R, S):
    return R + S

def difference(R, S):
    return [t for t in R if t not in S]

def intersect(R, S):
    return [t for t in R if t in S]

def project(R, p):
    return [p(t) for t in R]

def select(R, s):
    return [t for t in R if s(t)]
 
def product(R, S):
    return [(t,u) for t in R for u in S]

def aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k,v) in R if k == key])) for key in keys]

def dist(p, q):
    (x1,y1) = p
    (x2,y2) = q
    return (x1-x2)**2 + (y1-y2)**2


dis_list = []

for i in property_2014:
    
    for j in crime:
        dis_list += [(i[1], dist(i[1],j))]
 
        
agg = aggregate(dis_list,min)
print(agg)
