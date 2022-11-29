##You can run CDE CLI commands from the CML terminal, or embed in a script and run via CML Jobs, etc.


####Run Spark Job:

#You can run this either from the terminal or the CML prompt

!cde spark submit --conf "spark.pyspark.python=python3" /home/cdsw/data_extraction_scripts/Data_Extraction_Sub_150k.py

#If you submitted the above command from the CML Session prompt, you can open the terminal and run the following commands while the above still executes

####Check Job Status:

!cde run describe --id #, where # is the job id

####Review the Output:

!cde run logs --type "driver/stdout" --id #, where # is the job id e.g. 199


##In addition you can create a new CDE Resource:

#Create resource:

!cde resource create --name "cde_ETL" #Please edit the resource name here

#Upload file(s) to resource:

!cde resource upload --local-path "/home/cdsw/data_extraction_scripts/Data_Extraction*.py" --name "cde_ETL" #Please edit the resource name here

#Verify resource:

!cde resource describe --name "cde_ETL" #Please edit the resource name here



#Let's schedule two (2) jobs. The jobs have a dependency on an Apache Hive table, therefore we’ll schedule them a few minutes apart.

#Schedule the jobs:

#Please edit the job and resource names here (first and last option in each job)

!cde job create --name "Over_150K_ETL" --type spark --conf "spark.pyspark.python=python3" --application-file "/home/cdsw/data_extraction_scripts/Data_Extraction_Over_150k.py" --cron-expression "0 */1 * * *" --schedule-enabled "true" --schedule-start "2020-08-18" --schedule-end "2021-08-18" --mount-1-resource "cde_ETL"

!cde job create --name "Sub_150K_ETL" --type spark --conf "spark.pyspark.python=python3" --application-file "/home/cdsw/data_extraction_scripts/Data_Extraction_Sub_150k.py" --cron-expression "15 */1 * * *" --schedule-enabled "true" --schedule-start "2020-08-18" --schedule-end "2021-08-18" --mount-1-resource "cde_ETL" 
 

#Confirm scheduling:

!cde job list --filter 'name[like]%ETL%'