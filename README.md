# Facebook Stock Market Analysis

This repository contains the code and data files used to perform a stock market analysis. 
The purpose of this analysis is to identify patterns and trends in stock prices, 
as well as to assess the company performance prices based on historical data.


## Importing the data
The raw dataset can be downloaded [here](https://www.kaggle.com/borismarjanovic/price-volume-data-for-all-us-stocks-etfs) 

# Dataset 
 Main Focus : Facebook Stocks Data
For this analysis, I opted to use the Facebook Stocks Data. However, as the dataset was 
found to be relatively small during the exploratory data analysis, 
I decided to supplement it with additional data to enhance the analysis and gain a better understanding of the company's finances.

The data set can be accessed here https://www.kaggle.com/borismarjanovic/price-volume-data-for-all-us-stocks-etfs. Please not that the data set in this link is only for 2012- 2017 (last updated 11/10/2017).

#### The additional dataset(s) used: 
- Facebook Historical stock data from 2017 to 2023 
- Facebook yearly earning
- quartley earning
- NASDAQ index 

Next we will visualize stock movements for Facebook in the over the last 11 years 

### Exploratory Data Analysis

The first step was to perform an exploratory data analysis (EDA) to 
understand the content and structure of the data, identify errors, patterns, 
outliers, and relations among the variables. Notebook can be found in the same repo.
## steps to explore the data in details:
1. Upload the data file.
2. Inspect the schema to identify the data types and patterns.
3. Select a representative sample of the data to perform data quality analysis. No sampling required as the data set was manageable to hold on memory 
4. check the Count the number of null values in each column and create a list of columns with more than 75% null values.
5. Drop any columns with more than 75% null values. Not alot of the data was null
6. Identify any patterns, check data quality, visualize the data, 
and perform descriptive statistics to understand the nature of the data. 
7. Identify any data quality issues that require fixing or cleaning. For example, check for columns with incorrect data types, 
such as a string column for annual income, which should be numeric. Also, look for columns with additional string values,
such as months in a column labeled "terms," which could interfere with numerical statistics.

### Create AWS account and rds 
AWS free tier account was created, followed by the creation of a PostgreSQL database manually 
or programmatically. To ensure security, a virtual private network (VPC) and a security group were set up. 

Additionally, a PostgreSQL database was created with one replica in at least one more availability zone.
Next, AIM users were created with the necessary credentials to manage the PostgreSQL RDS database in AWS. 
This involved creating a user on the AWS Identity and Access Management (IAM) console and selecting the relevant 
policies needed for the database. Also creating the necessary security group and add inbound and outbound rule. 

Access keys were then generated for the user and the key was obtained for use in a code to connect to the database and create tables.

## Installation Instructions

The requirements located in the requirements file can be installed with pip in a python 3.8 virtual environment:

  *  requirements.txt - for a "bare bones" installation of the minimum requirements, allowing pip to find and install all dependencies


### Running the Job
The ETL job can be run by directly invoking the main.py script 

Using main.py:

```
main.py 
```
In order to run successfully, update the configuration file located in `config/` directory 
