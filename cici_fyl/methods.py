def union(R, S):
	return R + S

def difference(R, S):
	return [t for t in R if t not in S]

def intersect(R, S):
	return [t for t in R if t in S]

def project(R, p):
	return [p(t) for t in R]


#s is the value of the key
def select(R, s):
	temp=[]
	for l in R:
		if l["state"]==s:
			temp.append(l)

	return temp


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

#Calculate the edu score based on the nearby school types and numbers
def edu_score(R,school,distance):
	for l in R:
		if "location" in l:
			string=l["location"]
			string= string.replace("(","")
			string= string.replace(")","")
			stringlist= string.split(",")
			for i in range(len(stringlist)):
				stringlist[i]=float(stringlist[i])
			r1= stringlist[0]
			r2= stringlist[1]

			for i in school:
				num1=i["coordinates"][1]
				num2=i["coordinates"][0]

				if abs((num1-r1))<=distance and abs((num2-r2))<=distance:
					if i["school_type"]=="K-12":
						l["edu_score"]=l["edu_score"]+5
					elif i["school_type"]=="ES" or i["school_type"]=="MS" or i["school_type"]=="HS":
						l["edu_score"]=l["edu_score"]+1
					elif i["school_type"]=="K-8":
						l["edu_score"]=l["edu_score"]+2
					else:
						continue

		else:
			continue 
	return R



def averageincome(R):
	temp=[]
	if len(temp)==0:
		temp.append({"zipcode":R[0]["zipcode"],"income":0,"count":0})
	for l in R:
		for i in temp:
			found= False
			if i["zipcode"] == l["zipcode"]:
				found= True
				i["income"]= i["income"]+int(l["A02650"])
				i["count"] = i["count"]+1
		if found== False:
			temp.append({"zipcode":l["zipcode"],"income":int(l["A02650"]),"count":1})

	for x in temp:
		x["zipcode"]="0"+x["zipcode"]
		x["income"]=x["income"]/x["count"]

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