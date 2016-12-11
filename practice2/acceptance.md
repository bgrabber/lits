##Practice Acceptance##

  For this practice task will still require taking screenshots and attaching output of your application with test suite. You should fill all the tasks in the [practice acceptance document](https://docs.google.com/spreadsheets/d/1y50_13xftT4ATGkTItBwYbsufgdf2ExtrvDLmYen4Sw/edit?usp=sharing) to successfully pass the practice.

  **BIG WARN. Persons that haven't completed first practice won't have acceptance marks  until they'll finish first practice task.**


###Attaching logs###

  In second practice task there's a step when you must run your application and redirect the output into a file. There's an example in [practice document](https://github.com/marianfaryna/lits/blob/master/practice2/README.md) how to do that. You should attach the link to the log file in practice acceptance document.
```bash
/usr/java/jdk1.7.0_67-cloudera/bin/java -classpath your-jar-with-dependencies.jar your.entry.PointClass >> asdf.log
```
###Hbase shell tables###

  To list tables in Hbase, you should ssh into *cloundera.master* machine and launch Hbase shell. then execute command to list all tables.
```bash
ssh aws-user@cloudera.master
hbase shell
hbase(main):001:0> list
```      

![alt text](images/hbase-list-acceptance.png "Hbase shell list tables")

###Cloudera Manager Services###

  You need take a screenshot of Cloudera Manager services that include Solr and Lily Hbase Indexer services to show that you've added them and they are live.

  ![alt text](images/cloudera-manager-acceptance.png "Cloudera Manager services")

###Solr Querying###

  The last thing you should do, is to provide a screenshot of Solr query page with some results that you've indexed with Solr.
  To do that, go to Solr Service Web UI,select the collection and then go to Query tab. Hit the *Execute Query* button to execute basic query that should return all results from Solr. The result of a query **must not be empty**.

![alt text](images/solr-collection-acceptance.png "Solr collection selection")


![alt text](images/solr-query-acceptance.png "Solr query execution")

##Additional optional step##

In additional step you should check how different time is when executing Scan that loops through all the rows and the one that usus prefix scan where prefix is the Patient key part of Medical Record key. the difference should be significant when you have some data already in your Hbase storage. To execute additional step, you should just execute the code below in your entry point of application after test suite runs. Then attach the log of execution to the acceptance mark document.

```java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

Table medicalRecordsTable = null;
Connection connection = null;
try {
    Configuration configuration = new Configuration(true);
    configuration.set("hbase.zookeeper.quorum", "localhost");
    configuration.set("hbase.zookeeper.property.clientPort", "2181");
    connection = ConnectionFactory.createConnection(configuration);
    medicalRecordsTable = connection.getTable(TableName.valueOf(BaseTest.TABLES_MEDICAL_RECORD));


    com.lits.kundera.test.BaseTest.prefixScanTest("type", patientId, medicalRecordsTable);
} catch (IOException e) {
    System.out.println("Exception exetucing scans : " + e.getMessage());
} finally {
    try {
        if(medicalRecordsTable != null) {
            medicalRecordsTable.close();
        }

        if(connection != null && !connection.isClosed()) {
            connection.close();
        }

    } catch (IOException e) {
        System.out.println("Exception closing tables : " + e.getMessage());
    }

}
```
