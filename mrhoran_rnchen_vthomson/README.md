
PROJECT 1 Writeup:

  Project repository for the course project in the Spring 2017 iteration of the Data Mechanics course at Boston University.

  Write a short narrative and justification (5-10 sentences) explaining how these data sets can be combined to answer an interesting      
  question or solve a problem.

  We were interested in using data to see if certain privileges, namely healthy food options, correlated with certain socioeconomic and     healthly living trends. We wanted to look at things like the locations of community gardens, food pantries, and farmers' markets as   
  indications of healthier options in certain areas (by looking at available amounts by zip code). We then would try to crossreference  
  this with the demographics of certain areas to see what food options people in different socioeconomic situations have. We also thought   that medical situations (heart attacks, worse nutrition leading to injury, etc) might also give relevant information on the quality of   
  the areas around these healthier options. 
  Following intuition, we are hoping to see some sort of correlation between healthier eating options and healthier populations and in  
   turn higher living standard (more affluent areas might tend to have the luxury of healthier options, and perhaps the ability to be more 
   eat locally sourced foods for example). As for the medical events, perhaps we might see a higher amount of medical issues related to 
   worse nutrition (a disposition to cardiovascular issues, shorter life expecancy). Though we may need more data, we hope to see if 
   there's a positive correlation between the healthy choices and higher standards of living. Though it may not prove causation, the data 
   could be used to inform infrastructure, or even businesses (offering more healthy options depending on the results).

  Transformation 1: 
  In this first transformation we took the food pantries and community gardens and grouped them according to zipcodes and aggregated them. 
  And then we combined the lists into a new set by using projection to isolate the important data, selection to get the relevant data,    
  product to match up data by zipcode, and then projection and selection for formatting. After the transformations, the data shows the 
  zipcode, the number of community gardens and food pantries per areas.


  Transformation 2:
  In this second transformation we took farmers market data and found the location of the farmers market closest to Boston in the form  
  area,[lat,long]. Then we did K means to find the farmers market centers around Boston. 

PROJECT 2 Writeup:

  ** note trial is set to true for both transfromation_one_bus.py and transformation_two_bus.py
  ** All data is collected in the getbusData.py
  ** we are using the simulated students geojson file in the input data dir
  
  For this part of the project, we began to analyze the Boston Public School project. We wanted to focus on where students and schools are   located relative to busyards. There are a bunch of transformations to be fix this problem but for now we tackled two to get some 
  meaningful data.
  
  For our first transformation, we wanted to look at clustering of schools to find the hubs where the schools are located. We thought  
  this would be meanignful to see where the most schools are congregating, which could possibly influence where busyards are located or 
  potentially used to optimize bus routes around these hubs. So this optimization used Kmeans on the schools address: there are 89 
  coordinates in the schools.json file. So using this we wanted to test how many Ks or means gives us the best cost for the least amount 
  of means. In other words, we wanted to find how many means gave us the best distance to all the points. We found that about half the 
  number of points for means gaves us the best optimization, after that there was no dramatic benefit to the distance cost. This is 
  represented in the graph in kmeans_schools.png, where the means range from 1 to 89 ( the # of schools). In the trial mode, it is run on 
  half the data, taking a few seconds. So in our database we put 44 means into the database to show the school hubs. 
  
  In the second transformation, we wanted to do a statistical analysis of the average distance between students. This data would be useful 
  for buses to see how far they would have to travel between stops on average, in order for them to envision timing. Also, it would show  
  us where to place optimal bus stops for students, and the data is malleable to group students by different end and start times for  
  schools. In this transformation we use an rtree to do two groups: one grouping getting the average of the closest 10 students per 
  student, and one grouping based on the average distance in a .5 mile radius. We found the distance using vincenty distance, which takes 
  into account the earth's shape to find distance between coordinates. In trial mode, with the code split into 16ths, it runs in roughly 8 
  seconds. For the full set it will take 20 ish minutes. The sample outputs of the analysis are printed at the end.
  
