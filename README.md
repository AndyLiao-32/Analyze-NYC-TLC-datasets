# Analyze-NYC-TLC-datasets
Analyze New York City Taxi and Limousine Commission trips datasets - This Project was completed while taking the CSE 6242 courses in Georgia Tech.

Many modern-day datasets are huge and truly exemplify “big data”. For example, the Facebook social graph is petabytes large (over 1M GB); every day, Twitter users generate over 12 terabytes of messages; and the NASA Terra and Aqua satellites each produce over 300 GB of MODIS satellite imagery per day. These raw data are far too large to even fit on the hard drive of an average computer, let alone to process and analyze. Luckily, there are a variety of modern technologies that allow us to process and analyze such large datasets in a reasonable amount of time. For this project, I worked with a dataset of over 1 billion individual taxi trips from the New York City Taxi & Limousine Commission (TLC).

In Q1, I worked with a subset of the TLC dataset to get warmed up with PySpark. Apache Spark is a framework for distributed computing, and PySpark is its Python API. I used this tool to answer questions such as “what are the top 10 most common trips in the dataset”? The environment was defined by a Docker container.

In Q2, I performed further analysis on a different subset of the TLC dataset using Spark on DataBricks, a platform combining datasets, machine learning models, and cloud compute. This part was completed in the Scala programming language, a modern general-purpose language with a robust support 2 Version 0 for functional programming. The Spark distributed computing framework is in fact written using Scala.

In Q3, I used PySpark on AWS using Elastic MapReduce (EMR), and in Q4 I used Spark on Google Cloud Platform, to analyze even larger samples from the TLC dataset.

Finally, in Q5 I used the Microsoft Azure ML Studio to implement a regression model to predict automobile prices using a sample dataset already included in the Azure workspace.
