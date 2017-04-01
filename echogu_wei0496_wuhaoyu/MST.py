# This is the helper function to find MST among all student pick up points
# input:[cordinates] restricted by n(bus capacity, this is the maximum size of the tree)
import transformData
import math
import random
import statistics
from heapq import heappush, heappop
import cal_dis

# n is the capacity of the school bus! Change it here
# For a standard bus the capacity is 72, we assume 10 here for faster running time
n = 10

# convert the original input into format
def find_mst(input):
    res = transformData.aggregate(input, cal_MST)
    return res

def cal_MST(*points):
    points = points[0]
    # construct a adjacency matrix
    # Yield successive n-sized chunks from the input
    result = []
    for i in range(0, len(points), n):
        capacity = (points[i:i + n])
        ## issue: when capacity contains only one student, there is a bug
        if(len(capacity) != 1):
            adjacency_matrix = generate_graph(capacity)
            result += Prim(adjacency_matrix)
            # This is the pick up sequence of the student's index
            '''
            sequence = result[0]
            Total_dis = result[1]
            pickup_sequence = str(capacity[0][2])
            for i in range(1, len(sequence)):
                pickup_sequence += (' -> ' + str(capacity[i][2]))
            print('The pick up sequence is ' + pickup_sequence)
            print('The total travel distance of the route is ' + str(Total_dis) + 'miles')
            print('*******************************************************************')'''
    return result
    
# Initialization the adjacency matrix for the tree
def generate_graph(points):
    # initialize the adjacency matrix
    adjacency_matrix = [[100 for x in range(len(points))] for y in range(len(points))]
    for i in range(len(points)-1):
        for j in range(i+1, len(points)):
            adjacency_matrix[i][j] = cal_dis.distance(points[i][0:2],points[j][0:2])
            adjacency_matrix[j][i] = adjacency_matrix[i][j]
    return adjacency_matrix

# Initialization the adjacency list for the tree
# Complete graphs on n vertices, where the vertices are points chosen uniformly at random inside the unit
# square. (That is, the points are (x, y), with x and y each a real number chosen uniformly at random from
# [0, 1].) The weight of an edge is the Euclidean distance between its endpoints.

def generate_graph2(n):
    # initialize the adjacency matrix
    adjacency_matrix = [[100 for y in range(n)] for x in range(n)]

    # initailize the x,y value for all vertices
    x = [random.uniform(0,1) for x in range(n)]
    y = [random.uniform(0,1) for y in range(n)]

    # compute the euclidean distanace and initialize the weight
    for i in range(n-1):
        for j in range(i+1,n):
            adjacency_matrix[i][j] = math.sqrt((x[i] - x[j])**2 + (y[i]-y[j])**2)
            adjacency_matrix[j][i] = adjacency_matrix[i][j]

    return adjacency_matrix

# Run MST Prim's Algorithm
# Takes the random graph G as input and return the route and
# the total weight of the MST
def Prim(G):
    # print(G)
    # Initialize the final weight to 0
    result = 0
    # Initialize keys of all vertices as infinite
    adjacency_matrix = [[2] * len(G)] * len(G)
    # Initialize an empty priority queue 
    heap = []
    # Initialize an empty set of explored nodes S
    S = []
    # Insert source vertex into priority queue with key 0
    heappush(heap,[0,1])
    # Initialize an empty array which each index equals 1 if the node has been explored and 0 if not
    Lookup = [0] * len(G)
    # Insert remaining vertex into priority queue with infinity key
    for i in range(1,len(G)):
        heappush(heap,[2,i])

    while sum(Lookup) != len(G):
        u = heappop(heap)

        # ignore all subsequent duplicates
        if Lookup[u[1]] == 0:
        # if u[1] not in S:
            S += [u[1]]
            Lookup[u[1]] = 1
            result += u[0]
            # for each edge e = (u,v)
            for v in range(len(G)):
                # since in assumption it is a complete graph, we do not have to check if an edge exists
                #if v not in S:
                if Lookup[v] == 0:
                    # Since it's not efficient to lookup and modify existing tuples in the heap
                    # We can just insert the new tuple, upon removal we still have the lowest edge
                    heappush(heap,[G[u[1]][v],v])

    return S, result

























    
