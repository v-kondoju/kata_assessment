Assessment Requirement :- Data processing

Generate a csv file containing first_name, last_name, address, date_of_birth
Process the csv file to anonymise the data
Columns to anonymise are first_name, last_name and address
You might be thinking that is silly
Now make this work on 2GB csv file (should be doable on a laptop)
Demonstrate that the same can work on bigger dataset
Hint - You would need some distributed computing platform

####################################
#This project challenge has files following the given requirement of generating the csv file and anonymizing the sensitive data (columns - first_name, last_name, address),

Files/Scrpipts:
1. 'csv_datagen.py' - generating the csv files with random data
2. 'csv_anony.py'    - this script anonymize the data using pandas (small files)
3. 'spark_anony.py'  - this script anonymize the large datasets (real-time data)

   
