
# Presentation
This work is part of the course INFO8002-1 Topics in Distributed Systems.

By Henry Leclipteur, student at the university of Liège.

It was asked (A) to compute the separation degree of any actor and (B) answer the question: What is the average rating of movies per actor ?
Using Hadoop and Spark Scala.


The data we used is from the [IMDb datasets](https://developer.imdb.com/non-commercial-datasets/)

I will not explain here the details of the code and the algorithms used, only how to run them and get the results.

See the report for the details on code.


# Docker Set up
First we need to deploy an the HDFS-Spark cluster, run:

```
docker-compose up
```

While docker do its things, download the files [title.principals.tsv](https://datasets.imdbws.com/) and [title.ratings.tsv](https://datasets.imdbws.com/) and place them in the "workdir" directory (not mandatory but you will need to change directory to upload data to hdfs).

Once the HDFS-Spark cluster is deployed, let's create folders inside the namenode and hdfs:
```
docker exec -it namenode bash
mkdir Separation_Degree
mkdir Separation_Degree/Separation_Degree_classes
mkdir AvgRating
mkdir AvgRating/Separation_Degree_classes
hdfs dfs -mkdir -p /data/openbeer/WorkDir
hdfs dfs -mkdir -p /data/openbeer/WorkDir/inputs
exit
```
Note: All the command lines listed from here are made to work from the host terminal inside the directory "workdir".

Then upload the data we just downloaded from the host to hdfs:
```
docker cp title.principals.tsv namenode:title.principals.tsv
docker cp title.ratings.tsv namenode:title.ratings.tsv
docker exec -it namenode bash
hdfs dfs -put -f title.principals.tsv   /data/openbeer/WorkDir/inputs/title.principals.tsv  
hdfs dfs -put -f title.ratings.tsv   /data/openbeer/WorkDir/inputs/title.ratings.tsv  
exit
```

That's it we are all set up to run the different algorithms !

## Separation Degree using Hadoop
To calculate the separation degree of any actor, we will use the file "Separation_Degree.java".
It will compute the sepration degree of all actors given a certain actor (or a list of actor).

Here we will calculate the Streep number (Meryl Streep). 

Code of known actors:
* Meryl Streep  nm0000658
* Kevin Bacon   nm3636162

The code of any actor can be found in the [IMDb datasets](https://developer.imdb.com/) thanks to the file "name.basic.tsv".


First upload the file "Separation_Degree.java" and compile it:
```
docker cp Separation_Degree.java namenode:Separation_Degree/Separation_Degree.java
docker exec -it namenode bash
hdfs dfs -put -f Separation_Degree/Separation_Degree.java /data/openbeer/WorkDir/Separation_Degree.java
javac -d Separation_Degree/Separation_Degree_classes Separation_Degree/Separation_Degree.java -cp $(hadoop classpath)
jar -cvf Separation_Degree/Separation_Degree.jar -C Separation_Degree/Separation_Degree_classes/ .
```

Then remove the folder "output_sep_deg" (if existing) and run the code:
```
hdfs dfs -rm -r /data/openbeer/WorkDir/output_sep_deg
hadoop jar Separation_Degree/Separation_Degree.jar org.myorg.Separation_Degree /data/openbeer/WorkDir/inputs/title.principals.tsv /data/openbeer/WorkDir/output_sep_deg
```

Finally retreive the output from hdfs to the host using:
```
rm -r part-00000
hdfs dfs -copyToLocal /data/openbeer/WorkDir/output_sep_deg/Final/part-00000
exit
docker cp namenode:part-00000 output.txt 
```

And that's it, we have the separation degree of all actors from a given actor in "output.txt".

The ouput if of the form `SEP_ACTOR \t ACTOR \t DISTANCE`.

The `ACTOR` has a separation degree of `DISTANCE` from `SEP_ACTOR` and `SEP_ACTOR`, `ACTOR` are the codes of the actors.

A Distance of 2147483647 means the actor is not connected to the separation actor.

The output I generated is at `workdir/outputs/MR_Sep_Deg.txt`.

## Average Rating using Hadoop
To calculate the average rating of all actors, we will use the file "AvgRating.java".
It will compute the average rating for all actors (average rating of all films the actor played in).

First upload the file "AvgRating.java" and compile it:
```
docker cp AvgRating.java namenode:AvgRating/AvgRating.java
docker exec -it namenode bash
hdfs dfs -put -f AvgRating/AvgRating.java /data/openbeer/WorkDir/AvgRating.java
javac -d AvgRating/AvgRating_classes AvgRating/AvgRating.java -cp $(hadoop classpath)
jar -cvf AvgRating/AvgRating.jar -C AvgRating/AvgRating_classes/ .
```

Then remove the folder "output_avg_rating" (if existing) and run the code:
```
hdfs dfs -rm -r /data/openbeer/WorkDir/output_avg_rating
hadoop jar AvgRating/AvgRating.jar org.myorg.AvgRating /data/openbeer/WorkDir/inputs /data/openbeer/WorkDir/output_avg_rating
```

Finally retreive the output from hdfs to the host using:
```
rm -r part-00000
hdfs dfs -copyToLocal /data/openbeer/WorkDir/output_avg_rating/Final/part-00000
exit
docker cp namenode:part-00000 outputs/output.txt 
```

And that's it, we have the average rating of all actors in "output.txt".

The ouput if of the form `ACTOR \t AVGRATING`.

`ACTOR` is the code of the actor and `AVGRATING` is the average rating of the actor.

The output I generated is at `workdir/outputs/MR_AvgRating.txt`.


## Average Rating using Spark Scala
We will use here the file "AverageRating.scala".

To run the code:
```
docker exec -it spark-master bash
cd /opt/info8002/
/spark/bin/spark-shell --master spark://spark-master:7077
:load AverageRating.scala
```

The output is located in `workdir/outputs/SCALA_AvgRating.txt`

The ouput if of the form `ACTOR \t AVGRATING`.

`ACTOR` is the code of the actor and `AVGRATING` is the average rating of the actor.

The output I generated is at [`workdir/outputs/SCALA_AvgRating.txt`](https://github.com/Longferret/Docker-Hadoop-Spark/blob/master/workdir/outputs/SCALA_AvgRating.txt).



# Execution Times

| Program    | Execution Time |
| -------- | ------- |
| Separation_Degree.java  | 36m 20sec    |
| AvgRating.java | 4 min     |
| AverageRating.scala    | 58 sec     |

The specification of my laptop:
* Model: Predator PH315-53
* Processor: Intel(R) Core(TM) i7-10750H CPU @ 2.60GHz   2.59 GHz
* Ram: 16,0 Go
* OS: Windows 10


I made videos demonstrating the execution of each program:
* For Hadoop Separation degree see: [`workdir/videos/MR_SEP_DEG.tar.xz`](https://github.com/Longferret/Docker-Hadoop-Spark/blob/master/videos/MR_SEP_DEG.tar.xz)
* For Hadoop average rating see: [`workdir/videos/MR_AVG_RATING.tar.xz`](https://github.com/Longferret/Docker-Hadoop-Spark/blob/master/videos/MR_AVG_RATING.tar.xz)
* For Spark Scala average rating see: [`workdir/videos/SCALA_AVG_RATING.tar.xz`](https://github.com/Longferret/Docker-Hadoop-Spark/blob/master/videos/SCALA_AVG_RATING.tar.xz)


The analysis of the execution times are in the report here.

