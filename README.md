Project repository for the course project in the Spring 2017 iteration of the Data Mechanics course at Boston University.

Write a short narrative and justification (5-10 sentences) explaining how these data sets can be combined to answer an interesting question or solve a problem.

We were interested in using data to see if certain privileges, namely healthy food options, correlated with certain socioeconomic and healthly living trends. We wanted to look at things like the locations of community gardens, food pantries, and farmers' markets as indications of healthier options in certain areas (by looking at available amounts by zip code). We then would try to crossreference this with the demographics of certain areas to see what food options people in different socioeconomic situations have. We also thought that medical situations (heart attacks, worse nutrition leading to injury, etc) might also give relevant information on the quality of the areas around these healthier options. 
Following intuition, we are hoping to see some sort of correlation between healthier eating options and healthier populations and in turn higher living standard (more affluent areas might tend to have the luxury of healthier options, and perhaps the ability to be more eat locally sourced foods for example). As for the medical events, perhaps we might see a higher amount of medical issues related to worse nutrition (a disposition to cardiovascular issues, shorter life expecancy). Though we may need more data, we hope to see if there's a positive correlation between the healthy choices and higher standards of living. Though it may not prove causation, the data could be used to inform infrastructure, or even businesses (offering more healthy options depending on the results).

Transformation 1: 
In this first transformation we took the food pantries and community gardens and grouped them according to zipcodes and aggregated them. And then we combined the lists into a new set that shows the zipcode, the number of community gardens and food pantries per areas.

Transformation 2:
In this second transformation we took farmers market data and found the location of the farmers market closest to Boston in the form area,[lat,long]. Then we did K means to find the farmers market centers aroudn Boston. 
