# pystack-injester
This is a processor of the stack overflow data dump from XML into CSV.

The outputs are combined into a master dataframe with a user_id unique key.

## Acknowledgements
The boilerplate for this project was taken from [here](https://github.com/ekampf/PySpark-Boilerplate). 
but certain components have been removed in order to simplify the folder structure.

## How to run
In order to submit our job to spark we will have to package the code as there're a few moving pieces.
So if first downloading the project or pulling from repo run `make build` once in the main folder.

In order to submit the jobs to the cluster do so from the dist folder.
~~~~
cd dist && spark-submit --executor-memory 7G --py-files jobs.zip main.py --job badges_to_csv
~~~~

each job can be ran separately:
* badges_to_csv
* answers_to_csv
* comments_to_csv
* posthistory_to_csv
* postlinks_to_csv
* questions_to_csv
* users_to_csv

and the consolidation job:
* combine_from_csv

however for this last submission one needs to attach the spark-csv library

~~~~
cd dist && spark-submit --executor-memory 7G --packages com.databricks:spark-csv_2.10:1.5.0 --py-files jobs.zip main.py --job combine_from_csv
~~~~
