![Python 3.8](https://img.shields.io/badge/python-3.8-green.svg)
![Spark](https://img.shields.io/badge/Spark-2.4.5-green)

# Log Analysis on EDGAR Log File Data Set


# Table of Contents
1. [Background](README.md#Background)
2. [The Data](README.md#The-Data)
3. [Data Pipeline](README.md#The-Batch-Processing-Pipeline)
4. [Challenges](README.md#Challenges)
5. [Result](README.md#Result)
6. [Future Work](README.md#Future-Work)

# Background

The goal of this project is to use clustering algorithms to recognize log patterns in the EDGAR log files data set. By recognizing general log patterns from billions of logs, we can gain a better understanding of user activities.

# The Data

The Securities and Exchange Commission (SEC) is a collection of web server log files that allow researchers to study the demand for SEC filings. I ingested 500GB of log data, which is about 4.5 billion rows of log messages for batch processing. 

# The Batch Processing Pipeline

This is my batch processing pipeline. I used an EC2 instance to ingest the 2017 data from the SEC website to S3. Using an EMR cluster for batch processing, I pre-processed the data in Spark, then I did KMeans clustering and PCA for dimensional reduction and visualization. Then I stored the result in S3 and used Tableau for the front end.

![](images/Pipeline.png)

# Challenges

* The first issue was that CSV files are slow to be parsed in Spark. I transformed the data into a parquet columnar format, and reading the data into Spark became 6x faster.
* There were over 262K distinct documents viewed by ip addresses in the data. To make the data processing faster, I chose to focus on the most relevant features. So I ignored the documents that were viewed by < 1% of the users. Since these documents are only infrequently viewed, ignoring them will have minimal effect on the patterns that I discovered but significantly speed up the calculation by 23%.  
* I wanted to visualize the high-dimensional data. Ideally, I would use t-SNE, but it is computationally heavy. Due to the time and budget constraints, I used PCA for dimensionality reduction. Because the data is big, I had to perform PCA in a distributed way using Spark. Then I used t-SNE to produce a scatterplot on the dimensionally reduced result data set.

# Result: Compare Trends and Detect Anomalies

Here is the dashboard displaying some visualization of my log similarity analysis. On the left, you can see the dimensionally reduced clusters of users based on their activities. On the right, you can filter the cluster occupancies by date. Below, you can see monthly cluster occupancies to detect longer-term trends. For example, cluster 0’s occupancy increased from Q1 to Q2.

![](images/dashboard.png)

# Future Work

* Use Airflow to automate the pipeline.
* Include more feature types in the model and see how they affect the performance of the pipeline.
* To measure how well the clustering worked, ideally, we would apply the findings and update the website to see if users spend less time to obtain their requests and/or request more documents.

