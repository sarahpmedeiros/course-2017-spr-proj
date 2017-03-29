import shapefile
from pyproj import Proj, transform

myshp = open("zipcodes_nt/ZIPCODES_NT_POLY.shp", "rb")
mydbf = open("zipcodes_nt/ZIPCODES_NT_POLY.dbf", "rb")
r = shapefile.Reader(shp=myshp, dbf=mydbf)
shapes = r.shapes()
#print(shapes)

coor = []
for i in range(len(shapes)):
	coor += [0]

for i in range(len(shapes)):
	shape = shapes[i].points
	coor[i] = [shape]
#print(shape)

inProj = Proj(init='epsg:26986')
outProj = Proj(init='epsg:4326')
for i in range(len(coor)):
	for j in range(len(coor[i])):
		x1, y1 = coor[i][j][0], coor[i][j][1]
		#print(x1, y1)
		x2, y2 = transform(inProj, outProj, x1, y1)
		#print(y2[1])
		#print(x2[0])
		coor[i][j] = (y2[1], x2[0])
		#print(x2, y2)
a = [var[0] for var in r.records()]
print(a)

#print(coor1)
#print(len(coor))


#geo = [var.points for var in shapes]
#(zip[0],geo[0])
# b = {}
# b[zip[0]] = geo[0]