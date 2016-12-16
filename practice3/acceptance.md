##Practice Acceptance##

This practice acceptance will as usual, so no new steps will be introduced here. There will be three main goals for passing the practice:
  * Provided link for github repo where your python script with Spark code will be located. And as usual, all your result you should put in [acceptance document](https://docs.google.com/spreadsheets/d/1YRhLnorRICtTASVYawDaY8C8F_C9ilyhvD1oWI4Bss4/edit?usp=sharing)

  * Screenshot of Spark live service on Cloudera Manager.

  * And the last thing - attached log file as a result of your Spark code execution on cluster.

##Spark Service screenshot##

After you've installed Spark Service, you should see the service right on the main dashboard of Cloudera Manager. So you need to provide that screenshot which will show that your Spark service is up and running.

![alt text](images/park-history-server.png "Spark service cloudera")

##Spark execution logs##

So the same as we did in previous practice, you should redirect of the output of the application into file like in example below.

```python
spark-submit --class com.lits.spark.Main --master yarn --deploy-mode client /home/ec2-user/spark-test-1.0-SNAPSHOT-jar-with-dependencies.jar >> /home/ec2-user/app.log
```
After that, in acceptance document you should provide the link to log file.


##Congratulations##

Yo've finished that hard and cumbersome path that should have taught you basics of Hbase, Solr indexing and slight MapReduce and Spark. Hope practice was not that bad. And you enjoyed it somewhat. Thanks.

Marian
