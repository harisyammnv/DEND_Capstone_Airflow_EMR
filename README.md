#### Capstone Project

##### Data Sources from Udacity
U1) I94 Immigration Data: This data comes from the US National Tourism and Trade Office. A data dictionary is included in the workspace. This is where the data comes from. There's a sample file so you can take a look at the data in csv format before reading it all in. You do not have to use the entire dataset, just use what you need to accomplish the goal you set at the beginning of the project.

U2) World Temperature Data: This dataset came from Kaggle. You can read more about it here.

U3) U.S. City Demographic Data: This data comes from OpenSoft. You can read more about it here.

U4) Airport Code Table: This is a simple table of airport codes and corresponding cities. It comes from here.

#### Creating AWS CloudFormation Stack

In order to run this project the following resources are needed:
- `AWS EC2` - a `m4.xlarge` instance is needed for executing `Apache Airflow`
- `Apache Airflow` - for orchestrating the ETL pipeline
- `AWS RDS` - for Airflow to store its metadata
- `AWS S3` - for creating the Data Lake
- `AWS EMR` - cluster with `Apache Spark` and `Apache Livy` are needed to perform `Transformation` tasks on the raw data

To make the provisioning of the above resources easier, I have used the `AWS Cloudformation` to create
the stack needed for running this project.

The resources needed are already configured in the [airflow_server.yaml](./airflow_server.yaml)

To start creating the resource the following steps are to be performed:
1) Create an `AWS` account and provide necessary billing information
2) Create a `user` from `AWS IAM`
3) For the `user` make sure the following permissions are provided in IAM
![Permissions](./AWS_Help/permissions.PNG)
4) Finish the `user` creation step and  download the `AWS KEY ID` and `AWS SECRET` into a `csv` file
5) Create an `EC2 Key Pair` for accessing the EC2 instance for using `Airflow`

After finishing the above steps. Fill in the `dwh.cfg` with your details

Finally to create the cloud-formation stack use: `python create_resources.py`

#### Project Architechture
![Architechture](./AWS_Help/architechture.png)

##### External Data Sources
1) [Port Codes; Nationality Codes And Port of Entry codes](https://fam.state.gov/fam/09FAM/09FAM010205.html)
2) [US Non-immigrant Visa Types](https://www.dhs.gov/immigration-statistics/nonimmigrant/NonimmigrantCOA)
3) [US Immigrant Visa Types](https://en.m.wikipedia.org/wiki/Visa_policy_of_the_United_States#Classes_of_visas)
4) [Airline Codes](https://www.iata.org/en/about/members/airline-list?page=30&search=&ordering=Alphabetical)

##### Code References
1) [Developing Cloud Formation Script](https://github.com/aws-samples/aws-concurrent-data-orchestration-pipeline-emr-livy.git)
2) [Data pipeline Orchestration with Airflow, Spark and Livy](https://aws.amazon.com/blogs/big-data/build-a-concurrent-data-orchestration-pipeline-using-amazon-emr-and-apache-livy/)
