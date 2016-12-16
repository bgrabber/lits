package com.lits.spark

import java.util

import com.lits.kundera.test.{BaseTest, Util}
import org.apache.commons.lang.ArrayUtils
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.spark._
import org.apache.spark.rdd.RDD

object Main {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("HBaseRead").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val conf = HBaseConfiguration.create()
    val tableName = "medical_records"

    conf.set("hbase.zookeeper.quorum", "cloudera.master")
    conf.set(TableInputFormat.INPUT_TABLE, tableName)

    //we create RDD that will have our byte key from Hbase as the RDD key. For RDD value we will have Result object,
    // that contains all information related to that key(column families, columns and values)
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    // now we need to transform this RDD same way as we did with MapReduce.
    // We need to change create RDD with new key that wil look like patientId(bytes) + medical record type(bytes).
    // Fщr new RDD value we will have simple integer with value 1 if medical record type is not empty, and 0 if empty
    val updatedRDD: RDD[(ImmutableBytesWritable, Int)] = hBaseRDD.map((x) => {
      val result = x._2
      var tuple: (ImmutableBytesWritable, Int) = (new ImmutableBytesWritable(util.Arrays.copyOfRange(x._1.copyBytes(), 0, 16)), 0)
      if (!result.isEmpty) {
        val cell = result.getColumnLatestCell(BaseTest.TABLES_MEDICAL_RECORD.getBytes, BaseTest.COLUMN_MEDICAL_RECORD_TYPE.getBytes)
        if (cell != null && cell.getValueArray.length > 0) {
          val medRecType = new String(CellUtil.cloneValue(cell))
          tuple = (new ImmutableBytesWritable(ArrayUtils.addAll(util.Arrays.copyOfRange(x._1.copyBytes(), 0, 16), Bytes.toBytes(medRecType))), 1)
        } else {
          tuple = (new ImmutableBytesWritable(util.Arrays.copyOfRange(x._1.copyBytes(), 0, 16)), 0)
        }
      }
      //returning combined pair of key-value
      tuple
    })
//now we're aggregative value with the same key, so if key occured two times - the result value will be 1 + 1, if both keys had value 1
    val reducedRDD = updatedRDD.reduceByKey((x, y) => {
      x + y
    })
//printing out the result
    reducedRDD.foreach(x => {
      val patKey = util.Arrays.copyOfRange(x._1.get(), 0, 16)
      val medRecType = util.Arrays.copyOfRange(x._1.get(), 16, x._1.get().length)
      System.out.println("patient id " + Util.toString(patKey, patKey.length)
        + " : medical record type "
        + Bytes.toString(medRecType)
        + " - " + x._2)
    })

    //closing context
    sc.stop()
  }
}
