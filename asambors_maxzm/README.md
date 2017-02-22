# Project One

## CS591 Data Mechanics

### Ann Ming Samborski
### Max Mesirow 
#### Professor Lapets
###### 20 February 2017

The 5 datasets we chose to use were:
  * Hospital locations in Boston
  * Energy and water usage of buildings in Boston
  * A mapping from zipcodes to latitude and longitude
  * Zipcodes to average income
  * Demographic of people over the age of 18 that sleep less than 7 hours a night

From this data, our primary goal was to see if there was a relationship between sleeping less than seven hours a night and expected income. We also wished to see if the energy and water usage by hospitals in areas where people are getting less than seven hours of sleep a night was higher than areas that did not have a high number of sleep deprived. 

To do this, we aggregated hospitals by zipcode and then aggregated the energy output to observe if these results correlated with areas of people getting less than 7 hours of sleep. The most involved transformation was mapping the sleep data set to income. Without too much detail, this involved mapping latitude and longitude from the sleep dataset to zipcodes, and then mapping zipcodes to the corresponding expected incomes. Finally, we also mapped hospital location to income. It was particularly interesting for us to see areas like Jamaica Plain making 70K whereas Longwood/Fenway area, where MGH and some of the best known hospitals are, making only 30-40K. 

If this study were conducted more extensively, we believe it could add additional insight into the working habits of people who fall into the low, middle, and high income ranges as well as if there's a correlation to where they live and the healthcare that is provided (i.e. do wealthy people live in areas with easily available healtcare?). 