# course-2017-spr-proj
Project 1 by jw0208 (Wei Ji)

In SO215 sociology of healthcare, I encountered the concept of Social Economic Status (include three major factors: education, income, occupation), and it's importance in affecting people's
health. So for this project, I wanted to individually examine whether it is true that the level of education, the income and poverty
level really have a correlation with the health performance of a population. I am also interested in discovering whether it is true
that the more spent in Medicare per patient can really leads to better health performance (there is a theory that the more spent on
Medicare does not necessarily leads to better health performance), thus I also combined the population's physically and mentally
unhealthy days with the level of expense spent for Medicare. Eventually, I would wish to see some correlation between these data sets.
''
The five data sets:
1. the percentage of population that has high school diploma / bachelor degree within each state in year 2015 (https://data.ers.usda.gov/reports.aspx?ID=18243)
2. the median annual household income within each state in 2015 (https://data.ers.usda.gov/reports.aspx?ID=18242)
3. the percentage of poverty population within each state in 2015 (https://data.ers.usda.gov/reports.aspx?ID=14843#Pe155ccf8d4d44dadb55a19445080d1df_2_233iT3)
4. the per hospital Medicare spent per patient compare to national level within each state in year 2015 (https://catalog.data.gov/dataset/medicare-hospital-spending-per-patient-hospital-da578)
5. average physically and mentally unhealthy days in each state (https://chronicdata.cdc.gov/Health-Related-Quality-of-Life/HRQOL-Chart-of-Mean-physically-or-mentally-unhealt/fq5d-abxc)
''
Data Transformations:
''
healthEducation:
1. aggregate to calculate average education level for each state
2. combine education with health data (product 2 data sets->select the same state->project)
''
healthIncome:
1. combine income, poverty and health data (product 3 data sets->select the same state->project)
''
healthMedicare:
1. aggregate to calculate average Medicare spent for each state
2. combine Medicare with health data (product 2 data sets->select the same state->project)