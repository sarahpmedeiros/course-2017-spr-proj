from random import shuffle
from math import sqrt


def aggregate(R, f):
	keys = {r[0] for r in R}
	return [(key, f([v for (k,v) in R if k == key])) for key in keys]
	
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


#s is the value of the key
def select2(R, s):
	temp=[]
	for l in R:
		if l["state"]==s:
			temp.append(l)

	return temp

def distance_to_mean(l,R):
	min_distance=sqrt((R["coordinates"][0]-l[0][0])**2 + (R["coordinates"][1]-l[0][1])**2)
	for i in l:
		distance= sqrt((R["coordinates"][0]-i[0])**2 + (R["coordinates"][1]-i[1])**2)
		if distance < min_distance:
			min_distance = distance
		else:
			continue
	R["distance"] = min_distance
	return R


def selectcoordinate(R):
	temp=[]
	for l in R:
		if l["location"]["coordinates"][0]!=0 and l["location"]["coordinates"][1]!=0:
			temp.append(l["location"]["coordinates"])

	return temp

#Select needed info from school dataset
def schoolinfo(R):
	temp=[]
	for l in R:
		temp.append({"coordinates":l["geometry"]["coordinates"],"school_type":l["properties"]["SCH_TYPE"]})
	return temp


def merge(R, S): 
	temp=[]
	for k in R:
		temp.append(k)
	for l in temp: 
		for i in S:
			if l["zipcode"] == i["zipcode"]:
				l["income"]= i["income"]
	return temp

def merge1(R,S):
	temp=[]
	temp.append(R)
	temp.append(S)
	return temp

def appendattribute(R,attriname):
	temp=[]
	for j in R:
		temp.append(j)

	for l in temp:
		l[attriname]=0

	return temp

#Determine whether the property is in certain distance range
def inrange(R,coordinates,distance,countingattrib,key):
	for l in R:
		if key in l:
			
			string=l[key]
			string= string.replace("(","")
			string= string.replace(")","")
			stringlist= string.split(",")
			for i in range(len(stringlist)):
				stringlist[i]=float(stringlist[i])
			r1= stringlist[0]
			r2= stringlist[1]
			for x in range(len(coordinates)):
				num2=coordinates[x][0]
				num1=coordinates[x][1]

			if abs((num1-r1))<=distance and abs((num2-r2))<=distance:
				l[countingattrib]=l[countingattrib]+1
		else:
			
			continue


	return R 

def product(R, S):
	return [(t,u) for t in R for u in S]


def selectschoolcoordinates(R):
	return R["geometry"]["coordinates"]

def stringprocess(string):
	string= string.replace("(","")
	string= string.replace(")","")
	stringlist= string.split(",")
	for i in range(len(stringlist)):
		stringlist[i]=float(stringlist[i])
	r1= stringlist[0]
	r2= stringlist[1]
	return (r2,r1)

def stringprocess1(R):
	string= R["coordinates"]
	string= string.replace("(","")
	string= string.replace(")","")
	stringlist= string.split(",")
	for i in range(len(stringlist)):
		stringlist[i]=float(stringlist[i])
	r1= stringlist[0]
	r2= stringlist[1]
	R["coordinates"]=(r2,r1)
	return R

def dist(p, q):
	(x1,y1) = p
	(x2,y2) = q
	return (x1-x2)**2 + (y1-y2)**2

def plus(args):
	p = [0,0]
	for (x,y) in args:
		p[0] += x
		p[1] += y
	return tuple(p)

def scale(p, c):
	(x,y) = p
	return (x/c, y/c)

def kmeans(M,P):
	OLD = []
	compare=False

	while compare==False:
		OLD = M
		MPD = [(m, p, dist(m,p)) for (m, p) in product(M, P)]
		PDs = [(p, dist(m,p)) for (m, p, d) in MPD]
		PD = aggregate(PDs, min)
		MP = [(m, p) for ((m,p,d), (p2,d2)) in product(MPD, PD) if p==p2 and d==d2]
		MT = aggregate(MP, plus)
		
		M1 = [(m, 1) for ((m,p,d), (p2,d2)) in product(MPD, PD) if p==p2 and d==d2]
		MC = aggregate(M1, sum)
		M = [scale(t,c) for ((m,t),(m2,c)) in product(MT, MC) if m == m2]

		count=0
		OLD=sorted(OLD) 
		M=sorted(M)

		for i in range(len(OLD)):
			if abs(OLD[i][0] - M[i][0])<0.0001 and abs(OLD[i][1] - M[i][1])<0.0001:
				count=count+1
			else:
				break

		if (count==len(OLD)):
			compare=True

		print(sorted(M))
	return sorted(M)

#Correlations

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

def p(x, y,trial):
	c0 = corr(x, y)
	corrs = []

	if trial:
		for k in range(0, 500):
			y_permuted = permute(y)
			corrs.append(corr(x, y_permuted))
	else:
		for k in range(0, 5000):
			y_permuted = permute(y)
			corrs.append(corr(x, y_permuted))
	return len([c for c in corrs if abs(c) > c0])/len(corrs)




























