from random import shuffle
from math import sqrt

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

def map(f, R):
    return [t for (k,v) in R for t in f(k,v)]
    
def reduce(f, R):
    keys = {k for (k,v) in R}
    return [f(k1, [v for (k2,v) in R if k1 == k2]) for k1 in keys]

def normalizeDict(X, keyName, valName):
    justVals = [int(i.get(valName)) for i in X]
    justKeys = [i.get(keyName) for i in X]
    avg = sum(justVals)/len(justVals)
    normalizedVals = project(justVals, lambda x: x/avg)
    Y = [{"Zip Code":justKeys[i],"Crime Risk Index":normalizedVals[i]} for i in range(len(justKeys))]
    return Y    

def createTiers(X, A, k):
    minMax =  [(min([int(i.get(j)) for i in X]),max([int(i.get(j)) for i in X])) for j in A]
    Y = [0]*(k+1)
    for i in range(len(minMax)):
        interval = ((minMax[i][1] - minMax[i][0])/k)
        Y[i] = [minMax[i][0]+(interval*j) for j in range(k+1)]
    Z = [{A[j]:Y[j]} for j in range(len(A))]
    return Z

def zipToRent(X, A):
    Y = [(i.get("Zip "),i.get(A[j])) for i in X for j in range(len(A))]
    return Y

def assignTier(X, Y, A):
    housingTiers = Y[0].get(A[0])
    Z = [0]*len(X)
    for i in range(len(X)):
        for j in range(len(housingTiers)-1):
            if int(X[i][1]) >= int(housingTiers[j]) and int(X[i][1]) <= int(housingTiers[j+1]):
                Z[i] = {X[i][0]:housingTiers[j]}
                break
    return Z

def pullGrad(X, keyName, valName):
    justVals = [float(i.get(valName)) for i in X]
    justKeys = [i.get(keyName) for i in X]
    Y = [{"Zip Code":justKeys[i],"College_Grad_Rate":justVals[i]} for i in range(len(justKeys))]
    
    return Y

def pullAges(X, keyName, valName):
    justVals = [float(i.get(valName)) for i in X]
    justKeys = [i.get(keyName) for i in X]
    Y = [(justKeys[i],justVals[i]) for i in range(len(justKeys))]
    
    return Y

def pullNeighborhood(X, keyName, valName):
    justVals = [i.get(valName) for i in X]
    justKeys = [i.get(keyName) for i in X]
    Y = [{justKeys[i]:justVals[i]} for i in range(len(justKeys))]
    
    return Y

def maxRate(X):
    max1 = 0
    max2 = 0
    max3 = 0
    max4 = 0
    max5 = 0
    for i in range(len(X)):
        if X[i][0] == 1998:
            max1 = max(max1,X[i][1])
        if X[i][0] == 1806:
            max2 = max(max2,X[i][1])
        if X[i][0] == 1614:
            max3 = max(max3,X[i][1])
        if X[i][0] == 1422:
            max4 = max(max4,X[i][1])
        if X[i][0] == 1230:
            max5 = max(max5,X[i][1])
            
    
    tierToMax = [(1998,max1),(1806,max2),(1614,max3),(1422,max4),(1230,max5)]
    
    return tierToMax

def permute(x):
    shuffled = [xi for xi in x]
    shuffle(shuffled)
    return shuffled

def avg(x): # Average
    return sum(x)/len(x)

def stddev(x): # Standard deviation.
    m = avg(x)
    return sqrt(sum([(xi-m)**2 for xi in x])/len(x))

def cov(x, y): # Covariance.
    return sum([(xi-avg(x))*(yi-avg(y)) for (xi,yi) in zip(x,y)])/len(x)

def corr(x, y): # Correlation coefficient.
    if stddev(x)*stddev(y) != 0:
        return cov(x, y)/(stddev(x)*stddev(y))

def p(x, y):
    c0 = corr(x, y)
    corrs = []
    for k in range(0, 2000):
        y_permuted = permute(y)
        corrs.append(corr(x, y_permuted))
    return len([c for c in corrs if abs(c) > c0])/len(corrs)

    



    