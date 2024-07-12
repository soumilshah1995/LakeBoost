# LakeBoost: Maximizing Efficiency in Data Lake (Hudi) Glue ETL Jobs with a Templated Approach and Serverless Architecture


![image](https://user-images.githubusercontent.com/39345855/236826928-195dd3b6-ab36-4598-86e0-fa7deec1401c.png)

## Video : https://www.youtube.com/watch?v=DhMlFgn8UU4&t=20s
#### Article  Link : https://www.linkedin.com/pulse/lakeboostmaximizing-efficiency-data-lake-hudi-glue-etl-soumil-shah%3FtrackingId=P76LQeq7Shm2mOcFwRQrfg%253D%253D/?trackingId=P76LQeq7Shm2mOcFwRQrfg%3D%3D

## Introduction
In today's data-driven world, organizations are collecting and analyzing large amounts of data to gain insights and make informed decisions. As a result, the need for efficient and scalable big data solutions has increased significantly. Data lakes have emerged as a popular solution for storing and managing large amounts of structured and unstructured data.

However, managing data lakes can be challenging, especially when it comes to ingesting and processing large volumes of data. Extract, Transform, Load (ETL) jobs are a critical component of data lake management. ETL jobs are used to extract data from various sources, transform it to fit the data lake schema, and load it into the data lake.

Apache Hudi is an open-source data management framework that provides features such as incremental data processing and data change management for data lakes. AWS Glue is a fully managed ETL service that can run ETL jobs on data stored in AWS services such as Amazon S3, Amazon DynamoDB, and Amazon RDS. By using Apache Hudi and AWS Glue together, organizations can create a powerful data lake solution. In this paper, we'll discuss how we've maximized efficiency in our data lake Glue ETL jobs with a templated approach and serverless architecture.

Our data lake solution consists of the following components:
1. Data Storage: We use Amazon S3 to store data in our data lake. Amazon S3 is a highly scalable and durable object storage service that provides high availability and fault tolerance.
2. Apache Hudi: We use Apache Hudi to manage incremental data processing and data change management in our data lake. Apache Hudi provides features such as DeltaStreamer and Compaction that make data processing and management more efficient.
3. AWS Glue: We use AWS Glue to run ETL jobs on data stored in our data lake. AWS Glue is a fully managed ETL service that can automatically discover and catalog data, generate ETL code, and execute ETL jobs.
4. SQL Transformer: We have also included a SQL-based transformer in our data lake solution that allows for easy data transformation by passing SQL queries as input payloads.
5. Lambda Function: We use a Lambda function to trigger Glue ETL jobs based on metadata read from a DynamoDB table. The Lambda function is triggered on a CRON schedule and reads the metadata to determine the appropriate parameters for the Glue ETL job.
6. DynamoDB: We use DynamoDB to store metadata for our Glue ETL jobs. The metadata includes job-specific parameters such as input path, output path, and configurations for each job.
7. API-Based Microservice: We use an API-based microservice hosted on ECS to allow developers to interact with Swagger UI and set up new jobs for tables easily.
  
 ## Payload used to create Ingestion Job through Swagger UI 
![image](https://user-images.githubusercontent.com/39345855/236828356-161a1426-76ef-463e-ba0d-fd62b72936e2.png)


 ![image](https://user-images.githubusercontent.com/39345855/236828114-08f693b3-2aa5-48ce-a656-b23c368c7cab.png)

  
  
  
