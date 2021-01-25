# Aayush Sanjar

# Data Analysis on Stack Exchange Data

Data Analysis on Data from Stack Exchange
This project has been executed using Dataproc, a cloud service offered by Google which 
provides tools for batch processing, querying, streaming, and machine learning in Google 
Cloud Platform (GCP). The Google Cloud Dataproc system also includes a number of 
applications such as Hive, Mahout, Pig, Spark, and Hue built on top of Hadoop.
Task 1 – Get data from Stack Exchange
1. Visit website - https://data.stackexchange.com/stackoverflow/query/new
2. We want to obtain top 200,000 posts on the basis of view count. The Stack Exchange SQL 
console can only generate a maximum of 50,000 rows at a time. Hence, we need to write 4 
queries to obtain the result.
> SELECT * from posts where posts.ViewCount > 100000 ORDER BY posts.ViewCount 
DESC
> SELECT * from posts where posts.ViewCount <= 112523 AND posts.ViewCount > 
65000 AND posts.Id != 904910 ORDER BY posts.ViewCount DESC
> SELECT * from posts where posts.ViewCount < 66243 and posts.ViewCount > 42500 
ORDER BY posts.ViewCount DESC
> SELECT * from posts where posts.ViewCount <= 47290 and posts.ViewCount > 33000 
AND posts.Id NOT IN (488811, 14476448, 45351434, 2293592) ORDER BY 
posts.ViewCount DESC
3. Download the 4 .csv files – DataSet1, DataSet2, DataSet3, DataSet4 respectively.
4. We will upload the files on the GCP console and transfer them to Hadoop using the -put
command. Destination Folder - /user/hdfs
> hadoop fs -put ‘DataSet*.csv’ /user/hdfs
Task 2 – Perform Extract, Load and Transform (ETL) on the data
1. Open Grunt in the SSH console using the command 
> pig -x mapreduce
2. We will register and define piggybank in Pig to load all the 4 csv files from Hadoop to Pig
grunt> REGISTER /usr/lib/pig/piggybank.jar; 
grunt> DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();
3. Load records in Pig using the load command. (* indicates 1 or 2 or 3 or 4)
grunt> Data* = LOAD '/user/hdfs/DataSet*.csv' USING 
org.apache.pig.piggybank.storage.CSVExcelStorage(',', 
'YES_MULTILINE','NOCHANGE','SKIP_INPUT_HEADER') AS (Id:int, PostTypeId:int, 
AcceptedAnswerId:int, ParentId:int, CreationDate:chararray, DeletionDate:chararray, 
Score:int, ViewCount:int, Body:chararray, OwnerUserId:int, 
OwnerDisplayName:chararray, LastEditorUserId:int, 
LastEditorDisplayName:chararray, LastEditDate:chararray, 
LastActivityDate:chararray, Title:chararray, Tags:chararray, AnswerCount:int, 
CommentCount:int, FavoriteCount:int, ClosedDate:chararray, 
CommunityOwnedDate:chararray, ContentLicense:chararray);
4. Combine the data into one using union command.
grunt> data_combined = UNION data1, data2, data3, data4;
5. Perform the following commands to clean the data.
grunt> clean1 = FOREACH data_combined GENERATE Id, Score, ViewCount, OwnerUserId, 
OwnerDisplayName, REPLACE(Title,'<.*?>|\\t*\\r*\\n*\\s+',' ') AS Title, 
REPLACE(Body,'<.*?>|\\t*\\r*\\n*\\s+',' ') AS Body;
grunt> clean2 = FILTER clean1 BY ((OwnerUserId IS NOT NULL) AND (Score IS NOT NULL));
6. Store the cleaned data into a file called result using the store command.
grunt> STORE clean2 INTO 'result' USING 
org.apache.pig.piggybank.storage.CSVExcelStorage(',','YES_MULTILINE','NOCHANGE')
;
7. The result file is stored in 4 smaller parts. We need to merge them using hdfs -getmerge
command before running queries in Hive. New file – FinalDataSet.csv
> hadoop fs -getmerge hdfs://cluster-4d2f-m/user/aayush_sanjar2/result/part-m-00000 
hdfs://cluster-4d2f-m/user/aayush_sanjar2/result/part-m-00001 hdfs://cluster-4d2f￾m/user/aayush_sanjar2/result/part-m-00002 hdfs://cluster-4d2f￾m/user/aayush_sanjar2/result/part-m-00003 /home/aayush_sanjar2/fullcleanresult.csv
> hadoop fs -put 'fullcleanresult.csv' /user/aayush_sanjar2
Task 3 – Query using Hive
1. Open Hive and create an external table with same schema as FinalDataSet.csv. Table Name 
– datatab
hive> create external table if not exists datatab (Id int, Score int, ViewCount int, OwnerUserId 
int, OwnerDisplayName string, Title string, Body String) ROW FORMAT DELIMITED FIELDS 
TERMINATED BY ',';
2. Load the data from FinalDataSet.csv into datatab table.
hive> load data local inpath 'fullcleanresult.csv' overwrite into table datatab;
3. Perform the following queries: 
2
Query 1 - The top 10 posts by score.
hive> select id, score, owneruserid from datatab where score is NOT NULL order by score desc 
limit 10;
Query 2 - The top 10 users by post score.
hive> select owneruserid,sum(score) AS totalscore from datatab where OwnerUserId is not 
NULL group by owneruserid order by totalscore desc limit 10;
Query 3 - The number of distinct users, who used the word “Hadoop” in one of their posts.
hive> select count(distinct(owneruserid)) from datatab where lower(body) like '%hadoop%';
Task 4 – Calculate per user TF-IDF (based on Query 2)
1. Get the contents of query 2 into a file hiveoutput and put it in Hadoop for implementing 
Map Reduce
hive> CREATE TABLE TopUsers ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' AS 
SELECT OwnerUserId, SUM(Score) AS TotalScore FROM datatab GROUP BY OwnerUserId 
ORDER BY TotalScore DESC LIMIT 10;
hive> CREATE TABLE TopUserPosts AS SELECT OwnerUserId, Body, Title FROM datatab 
WHERE OwnerUserId in (SELECT OwnerUserId FROM TopUsers) GROUP BY OwnerUserId, 
Body, Title;
hive> INSERT OVERWRITE DIRECTORY '/user/aayush_sanjar2/hiveoutput' ROW FORMAT 
DELIMITED FIELDS TERMINATED BY ',' SELECT OwnerUserId, Body, Title FROM TopUserPosts 
GROUP BY OwnerUserId, Body, Title;
2. TF-IDF in Hadoop is implemented using Python in three phases with three mappers and 
three reducers. Use the following commands to run TF-IDF on the cluster.
> hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -file MapperPhaseOne.py 
ReducerPhaseOne.py -mapper "python MapperPhaseOne.py" -reducer "python 
ReducerPhaseOne.py" -input hdfs://cluster-4d2f￾m/user/aayush_sanjar2/hiveoutput/000000_0 -output hdfs://cluster-4d2f￾m/user/aayush_sanjar/tfidftab/R1
> hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -file MapperPhaseTwo.py 
ReducerPhaseTwo.py -mapper "python MapperPhaseTwo.py" -reducer "python 
ReducerPhaseTwo.py" -input hdfs://cluster-4d2f-m/user/aayush_sanjar/tfidftab/R1/part-
00000 hdfs://cluster-4d2f-m/user/aayush_sanjar/tfidftab/R1/part-00001 hdfs://cluster- 4d2f-m/user/aayush_sanjar/tfidftab/R1/part-00002 -output hdfs://cluster-4d2f￾m/user/aayush_sanjar/tfidftab/R2
> hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -file MapperPhaseThree.py 
ReducerPhaseThree.py -mapper "python MapperPhaseThree.py" -reducer "python 
ReducerPhaseThree.py" -input hdfs://cluster-4d2f-m/user/aayush_sanjar/tfidftab/R2/part- 3
00000 hdfs://cluster-4d2f-m/user/aayush_sanjar/tfidftab/R2/part-00001 hdfs://cluster-
4d2f-m/user/aayush_sanjar/tfidftab/R2/part-00000 -output hdfs://cluster-4d2fm/user/aayush_sanjar/tfidftab/R3
3. The resultant file – R3 is subdivided into smaller parts. We need to merge the files to get 
the final processed file
> hadoop fs -getmerge /user/aayush_sanjar/tfidftab/R3/part-00000 
/user/aayush_sanjar/tfidftab/R3/part-00001 /user/aayush_sanjar/tfidftab/R3/part-00002 
/home/aayush_sanjar2/mergedfinal
4. Now we convert the final file into csv format for final hive processing.
> sed -e 's/\s/,/g' mergedfinal > output.csv
5. Open hive and write the following commands to display the TF-IDF result.
hive> create external table if not exists TFIDF (Term String,Id int,tfidf float) ROW FORMAT 
DELIMITED FIELDS TERMINATED BY ','; load data local inpath 'output.csv' overwrite into 
table TFIDF;
hive> SELECT * FROM (SELECT ROW_NUMBER() OVER(PARTITION BY Id ORDER BY tfidf 
DESC) AS TfidfRank, * FROM TfIDF) n WHERE TfidfRank IN (1,2,3,4,5,6,7,8,9,10);
References
1. The entire TFIDF Python code was referred from this githttps://github.com/SatishUC15/TFIDF-HadoopMapReduce#tfidf-hadoop with minor 
changes (i.e. addition of stop words and the related if condition in MapperPhaseOne.py)
2. Stack Overflow: https://stackoverflow.com/
3. DataProc Documentation https://cloud.google.com/dataproc/docs
