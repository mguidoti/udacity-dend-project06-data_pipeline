

<p align="center"><a href="https://www.udacity.com/course/data-engineer-nanodegree--nd027">Udacity Data Engineer Nanodegree</a></p>

# Project 06: Data Pipeline

The information in the Project Overview and Datasets subsections are copied/pasted from the Udacity's project page. In order to highlight that, I'm adding them as citations blocks in this README.


## Project Overview
> This project will introduce you to the core concepts of Apache Airflow. To complete the project, you will need to create your own custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.

## Architecture & Code
This project ran on Amazon's Redshift as the data warehouse, and in Apache Airflow installed locally. All ETL scripts were written in SQL.

### Modifications on the Provided Template
1. The `Begin_execution` task was modified to become a SubDAG that will trigger all create statements for all tables, including staging tables
2. The create statements were moved into a class on `plugins/helpers`
3. The create statements were modified to include `IF NOT EXISTS`
4. The create statement class received an additional dictionary containing the name of the tables and their respective create statement
5. The SQL statements on SQLQueries class were modified to include `INSERT INTO <TABLE>`

## Datasets

> For this project, you'll be working with two datasets. Here are the s3 links for each:
>
> Log data: `s3://udacity-dend/log_data`
> Song data: `s3://udacity-dend/song_data`


## Data Schema

### Transitional/Staging Tables

#### staging_events (transitional)

|Column|Type|Spec.|
|--|--|--|
|event_id|INTEGER|IDENTITY(0,1) NOT NULL PRIMARY KEY DISTKEY|
|artist|VARCHAR||
|auth|VARCHAR||
|firstName|VARCHAR||
|gender|VARCHAR||
|itemInSession|INTEGER|| 
|lastName|VARCHAR||
|length|FLOAT||
|level|VARCHAR||
|location|VARCHAR||
|method|VARCHAR||
|page|VARCHAR||
|registration|BIGINT||
|sessionId|INTEGER||
|song|VARCHAR||
|status|INTEGER||
|ts|TIMESTAMP||
|userAgent|VARCHAR||
|userId|INTEGER||

#### staging_songs (transitional)

|Column|Type|Spec.|
|--|--|--|
|num_songs|INTEGER|NOT NULL PRIMARY KEY DISTKEY||
|artist_id|VARCHAR|NOT NULL|
|artist_latitude|DECIMAL||
|artist_longitude|DECIMAL||
|artist_location|VARCHAR||
|artist_name|VARCHAR||
|song_id|VARCHAR||
|title|VARCHAR||
|duration|DECIMAL||
|year|INTEGER||

### Fact Table

#### songplays

|Column|Type|Spec.|
|--|--|--|
|songplay_id|INTEGER|IDENTITY(0,1) NOT NULL PRIMARY KEY DISTKEY|
|start_time|TIMESTAMP|NOT NULL|
|user_id|VARCHAR||
|level|VARCHAR||
|song_id|VARCHAR||
|artist_id|VARCHAR||
|session_id|VARCHAR||
|location|VARCHAR||
|user_agent|VARCHAR||


### Dimension Tables

#### users

|Column|Type|Spec.|
|--|--|--|
|user_id|INTEGER|NOT NULL PRIMARY KEY DISTKEY|
|first_name|VARCHAR||
|last_name|VARCHAR||
|gender|VARCHAR||
|level|VARCHAR||

#### songs

|Column|Type|Spec.|
|--|--|--|
|song_id|VARCHAR|NOT NULL PRIMARY KEY DISTKEY|
|title|VARCHAR||
|artist_id|VARCHAR||
|year|INTEGER||
|duration|DECIMAL||

#### artists

|Column|Type|Spec.|
|--|--|--|
|artist_id|VARCHAR|NOT NULL PRIMARY KEY DISTKEY|
|name|VARCHAR||
|location|VARCHAR||
|latitude|DECIMAL||
|longitude|DECIMAL||

#### time

|Column|Type|Spec.|
|--|--|--|
|start_time|TIMESTAMP|NOT NULL PRIMARY KEY SORTKEY DISTKEY|
|hour|INTEGER||
|day|INTEGER||
|week|INTEGER||
|month|INTEGER||
|year|INTEGER||
|weekday|INTEGER||

## Validation

### DAG (and SubDAG) Graph Views
#### DAG
![](https://i.imgur.com/yM7CDO1.png)

#### SubDAG
![](https://i.imgur.com/iiLxr2n.png)


### Validation Code

```sql
SELECT count(*) AS count_staging_songs,
	   (SELECT COUNT(*) FROM dev.public.staging_events) AS count_staging_events,
       (SELECT COUNT(*) FROM dev.public.artists) AS count_artists,
       (SELECT COUNT(*) FROM dev.public.users) AS count_users,
       (SELECT COUNT(*) FROM dev.public.songs) AS count_songs,
       (SELECT COUNT(*) FROM dev.public.time) AS count_time,
       (SELECT COUNT(*) FROM dev.public.songplays) AS count_songplays
FROM dev.public.staging_songs;
```
#### Results in Number of Rows
|count_staging_songs|count_staging_events|count_artists|count_users|count_songs|count_time|count_songplays|
|--|--|--|--|--|--|--|
14896|8056|10025|104|14896|6820|6820|

## Files Included
```
dags/
--|__init__.py
--|Guidoti_DAG_prj6.py............................the DAG file
--|Guidoti_SubDAG_prj6.py.........................the SubDAG file
plugins/
--|helpers/
----|__init__.py..................................allows import of helpers
----|create_tables.py.............................all CREATE statements
----|data_quality.py..............................parametrized data quality checkers
----|sql_queries.py...............................all INSERT statements
--|operators/
----|__init__.py..................................allows import of operators
----|data_quality.py..............................DataQualityOperator
----|load_dimension.py............................LoadDimensionOperator
----|load_fact.py.................................LoadFactOperator
----|stage_redshift.py............................StageToRedshiftOperator
--|__init__.py
```


## How to Run
First you need to make sure you have access to an instance of Apache Airflow and that this instance is accepting outbound network connections. Then, Add your AWS credentials and Redshift Cluster information as connections of your Airflow Instance. Finally, you can simply turn the DAG on and trigger it.


## Disclaimer
This is a study repo, to fulfil one of the project-requirements of the [Udacity Data Engineer Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027), submitted on December/2021. This has no intention to continue to be developed, and the git commit messages follow the [Udacity Git Style Guide](https://udacity.github.io/git-styleguide/), with the addition of the keyword `file` (used when moving non-coding files).


