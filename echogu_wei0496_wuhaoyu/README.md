# Project 2: Modeling, Optimization, and Statistical Analysis
Team: [Lingyi Gu][lyg], [Wei Wei][ww], [Haoyu Wu][hyw]

## Objective
The [Boston Public Schools][bps] has announced a [Transportation Challenge][tc].


## Optimization Methods
### Assigning Students to Buses
###### Number of buses

###### K-means Clustering Algorithm

Problems that we cannot solve for now:
1. K-means with same cluster size.
Although we set the number of buses to 50, our algorithm cannot assign students evenly to those buses.

### Bus Route
###### Minimum Spanning Tree


## Trial Mode
###### assignStudents

###### optimizeBusRoute
Replying on the output of assignStudents algorithm, a trial mode is not needed.

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
$ python execute.py echogu_wei0496
```

[lyg]: https://github.com/lingyigu
[ww]: https://github.com/wei0496
[hyw]: https://github.com/wuhaoyujerry
[bps]: http://www.bostonpublicschools.org/
[tc]: http://bostonpublicschools.org/transportationchallenge
[dm]: https://github.com/Data-Mechanics/course-2017-spr-proj
