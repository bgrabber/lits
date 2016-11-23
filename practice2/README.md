##Main practice tasks##

  The main goal for this practice is to get more familiar with Hbase concepts. We'll start with real-world problem field and describe it with Hbase schema. Then we'll create Hbase tables and fill them with data using ORM framework Kundera. Add indexes to Hbase records using Lily-Hbase-Indexer and Solr services. After we'll have the data in Hbase, we'll write map reduce job that will count data by specific field.To sum up:

  * Describe problem field
  * Create Hbase schema with Kundera ORM.
  * Fill Hbase storage with data
  * Set up indexing using Lily and solr
  * Write MapReduce job to analyze data

## Problem field ##

  * Medical Field
    At the top level we have a **Physician** that works for some clinic and  accepts patients. Physician also has speciality and common data like name, surname and age. **Patient** that is accepted by Physician, has common demographics data like name, surname and date of birth. All the Patient's medical history is stored within medical records. **Medical Record** is a an atomic event in Patient's life that related to medical sphere, such as diagnosis, examination, visit to doctor, pills prescription. So medical record has type(exam, diagnosis, visit, prescription), date when the action was performed and description of action of medical record.

##Creating Java application that will contain your actual practice##

  So to start with Kundera at first we need to create Java project using Maven(as it's most comfortable way to arrange project in Java world). So using IDE(like IDEA, Eclipse or Netbeans) or manually, create project with proper folder structure and **pom.xml** configuration file where you'll store your classes that will describe your Hbase entities. Below I provided description for entities that should be implemented with Kundera. Additionally I put some pitfalls that you should consider when using Kundera. If you'll have some other difficulties or problems, you can pm me in Slack.

##Implementing Hbase Schema with Kundera ORM##

  To describe problem field entities with [ORM](https://en.wikipedia.org/wiki/Object-relational_mapping) framework(we will use [Kudera](https://github.com/impetus-opensource/Kundera) for this). Kundera is swiss knife for data access to different storages such as MongoDB, Cassandra, Hbase, etc. Using Kundera documentation on Github, you should implement entities described in problem field. For a hint and tests purposes, I provided Entity names and some metadata that should be present in your code. This is very important as we'll have test suite that will check whether Hbase schema data is properly set in storage. So be careful reading practice task and implementing your entities!

    * Physician
      * Table name **physicians**
      * Column **id** UUID as byte[]
      * Column **full_name** Name and Surname as String
      * Column **clinic_name** Name of clinic as String
      * Column **specialization** Name of physicians specialization(therapist, dentist, etc.) as String
    * Patient
      * Table name **patients**
      * Column **id** UUID as byte[]
      * Column **first_name** Name as String
      * Column **last_name** Surname as String
      * Column **date_of_birth** patient's date of birth as java.util.Date
    * Medical Record
      * Table name **medical_records**
      * Column **id** UUID of patient concatenated with UUID of medical record as byte[]
      * Column **type** Type of record (doctor's visit, exam, prescription, etc.) as String
      * Column **description** Record description as String
      * Column **date_performed** date when record was performed as java.util.Date

    * **IMPORTANT!**. Few things that you should consider when implementing entities:
      * in **Medical Record** UUID is a concatenation of keys. DO not use **@EmbeddedId** for concatenation. Better to use simple **@Id** with byte array and set it with *ArrayUtils.addAll(patient_key,medical_record_key)*
      * Kundera can connect to default Zookeeper port. To change zookeeper host and port, you should add Hbase-specific configuration file in XML format with content as described below:

      {code}
      <?xml version="1.0" encoding="UTF-8"?>
      <clientProperties>
        <datastores>
          <dataStore>
            <name>hbase</name>
            <connection>
                <properties>
                    <property name="hbase.zookeeper.quorum" value="your.zookeeper.host"></property>
                    <property name="hbase.zookeeper.property.clientPort" value="your.zookeeper.port"></property>
                </properties>
            </connection>
         </dataStore>
        </datastores>
      </clientProperties>
      {code}

      Also you need to add this file to your *persistence.xml* file:

      {code}
      <property name="kundera.client.property" value="name_of_your_xml_with_properties.xml" />
      {code}

      * When adding **@Table** your should put *schema* property you should put **default@your_persistence_unit** do that tables will be created in default Hbase namespace.

      * When adding property *kundera.ddl.auto.prepare* to your persistence.xml, put value **update** instead of **create**, as Kundera will try to delete reserved **default** namespace in Hbase.




##Running main program with tests##

After we've prepared schema for Hbase we need to test whether our schema was properly configured and working. To do this, we'll put some data into Hbase using Kundera and test if data was put successfully into Hbase. To start with, we'll create simple entry point of an application and also a test class to check whether everything is working properly. After we'll implement entry point and test class, we'll run the created appliction on cluster to actually check if everything is working.

  * Creating tests
  * Creating entry point
  * Adding more configuration(plugins)
  * Running the application(saving results)

  * tables exists;
  * column family present;
  * columns present;
  * result is not empty in table;
  * result contains all values in all columns;
