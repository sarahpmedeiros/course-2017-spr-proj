# Project 2: Modeling, Optimization, and Statistical Analysis
Team: [Lingyi Gu][lyg], [Wei Wei][ww], [Haoyu Wu][hyw]

## Objective
The [Boston Public Schools][bps] has announced a [Transportation Challenge][tc].  Our group is really interested in this challenge and starts this project. It contributes to Step 2 of the Routing Challenge, which aims to improve the school bus routing efficiency.  Specifically, we want to optimize the route for each school bus to get all students  to school as quickly as possible. We use the data set  of students who attend Boston Public Schools and optimize the bus routes based on their locations. Detailed methods are listed as follow.

## Optimization Methods
### Overall Assumption
Consider the project deadline and our capability, we made a few assumptions for the sake of simplicity:
1. We assume that students are picked up from their home location.
2. We assume that all the schools have the same bell time.
3. We also assume that each bus has unlimited capacity.

### Assigning Students to Buses
We retrieve the students’ coordinations and schools they attend from the dataset and aggregate by school names.
#### Number of buses
We set each school bus to have 50 seats, which is same as a normal bus size.  Therefore, we have a general idea of how many buses we need for each school.
#### K-means Clustering Algorithm
In order to make sure each bus has shortest route, it needs to pick students that live near to each other. Therefore, we implement the [k-mean clustering](https://en.wikipedia.org/wiki/K-means_clustering)  algorithm to group the students based on their location. Then we find the mean point in this cluster and store into the new data set. Because we cannot control how many students are assigned into each cluster, we cannot  guarantee that each bus have less than 50 students.

### Bus Route
We use the new data obtained from the previous algorithm.
#### Minimum Spanning Tree
We implement the [Minimum Spanning Tree](https://en.wikipedia.org/wiki/Minimum_spanning_tree) algorithm to find the relatively minimum distance for each bus to pick all the students in that cluster.
###### Special Cases:
Some students live very close to each other so they have the same coordination in the dataset. Therefore, the Minimum Spanning Tree will return 0 as the total distance in some cases,.

### Running Time
Because K-means Clustering is a NP-Hard problem, it takes approximately 15 minutes to the entire project.

## Trial Mode
###### Run
```
$ python execute.py echogu_wei0496_wuhaoyu --trial
```
###### assignStudents
In the trial mode, we select students from ```k``` schools randomly. You can set the number of schools by changing the value of ```k``` in the following line.
```python
if trial:
    school_students = random.choices(school_students, k = 1)
```

## Setting Up

Follows the procedure in [Data-Mechanics/course-2017-spr-proj][dm].

###### Required libraries
```
$ pip install prov --upgrade --no-cache-dir
$ pip install dml --upgrade --no-cache-dir
$ pip install protoql --upgrade --no-cache-dir
$ pip install geopy
```
###### Run
```
$ python execute.py echogu_wei0496_wuhaoyu
```

[lyg]: https://github.com/lingyigu
[ww]: https://github.com/wei0496
[hyw]: https://github.com/wuhaoyujerry
[bps]: http://www.bostonpublicschools.org/
[tc]: http://bostonpublicschools.org/transportationchallenge
[dm]: https://github.com/Data-Mechanics/course-2017-spr-proj
