# SPARKIFY ETL (Amazon Redshift)

## Table of Contents

- [SPARKIFY ETL (Amazon Redshift)](#sparkify-etl-amazon-redshift)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [Tools](#tools)
  - [Usage](#usage)
  - [Project Files](#project-files)

## Introduction

A music streaming startup, **Sparkify**, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in **S3**, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The project is to build an ETL pipeline that extracts their data from **S3**, stages them in **Redshift**, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.

## Tools

<p style="float:left">
<img src='./images/python.svg' alt="Python" title="Python"/>
<img src='./images/psycopg.svg' alt="Psycopg2" title="Psycopg2" width="60"/>
<img src='./images/aws-redshift.png' alt="Amazon Redshift" title="Amazon Redshift"/><img src='./images/aws-s3.png' alt="s3" title="s3"/>
</p>
<div style="clear:both">

## Usage

* First, you have to create the redshift cluster and the IAM Role to stage the data in it by following the `Cluster_Create_Delete.ipynb` instructions
  
* Second, to run the ETL we have to create the database and the tables so we have to execute `create_tables.py` file.
* `Create_tables.py` creates 2 tables for staging the log and song files from _**S3**_ into _**AWS Redshift**_ and 4 dimensions and a fact table for the star schema data warehouse

* Now we can run `etl.py` to transfer our data from songs and log files into the staging database tables then transfer the data from staging tables to the data warehouse dimensions and fact.

**_Important_**: _update the database and account credentials in the files to your own credentials in `dwh.cfg`

## Project Files

* `Cluster_Create_Delete.ipynb`
  * This jupyter notebook is used to create the cluster and deleting it after completing
  * It is a series of instructions to create the cluster and the roles associated to it
  * The file gets its credentials from `dwh.cfg` so you have to fill the required credentials first

* `dwh.cfg`
  * Create your IAM User with Admin Access and put your credentials in this file
  * Put your cluster credentials and number of nodes you want to create in `Cluster_Create_Delete.ipynb` in this file
  * **_Important_**: Don't forget to remove your credentials before publishing the project

* `create_tables.py`
  * You run this python script after creating the cluster
  * This file creates the 2 staging tables which is **_staging_songs_** and **_staging_events_** tables to store the s3 staged json log and songs data into them
  * This file creates the 4 dimensions and fact table to transfer the data in the staging tables into it
  * The queries that execute to run this script is in `sql_queries.py`

* `etl.py`
  * After we create the cluster and the tables we call this script to transfer the data from the s3 json files to the 2 staging tables created
  * Then it transfers the data from the 2 staging tables to the data warehouse dimensions (4 dimensions) and fact.

* `sql_queries`
  * This file contains the queries executed to create and drop the tables, transfering the data into the 2 staging tables then the queries to transfer the data into the data warehouse.




