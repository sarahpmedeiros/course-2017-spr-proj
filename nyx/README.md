Farmer Market and Landmarks Analysis

Data Resource
———————————————————————
This project is designed for visitors to get suggestion about taking short trips in Boston among farmer markets and landmarks. There are six data sets total:

1. Boston Landmarks: https://data.cityofboston.gov/resource/u6fv-m8v4.json

2. Farmer Markets: https://data.mass.gov/resource/66t5-f563.json

3. School Gardens: https://data.cityofboston.gov/resource/pzcy-jpz4.json

4. Crime Info: https://data.cityofboston.gov/resource/29yf-ye7n.json

5. Parking: https://data.cityofboston.gov/resource/gdnf-7hki.json

6: Police Station: https://data.cityofboston.gov/resource/pyxn-r3i2.json



Transformation
————————————————————————
For the transformation part, there are three transformations total: 

1. The first transformation is designed for merging landmarks and farmer markets to get the total visitor options.

2. The second transformation is used for let visitors know the farmer markets that have police station within two miles.

3. The third transformation is for suggesting visitor all the landmarks and farmer markets that have parking area.



Other Instruction
—————————————————————————
You need to ‘pip3 install geopy’ before doing ‘python3.6 execute.py nyx’.



Potential Function
——————————————————————————
In the future, it could be added more functions, such as suggesting safety report combined with crime reports for the visitor areas, and give a rating score. Furthermore, it could also add visitor reviews for new visitors. 


