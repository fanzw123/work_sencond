package hbase

import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, HTable}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Administrator on 2016/1/13.
 */
class thr_dico {
  val conf = new SparkConf().setAppName("Thr Dico")
  conf.set("spark.kryoserializer.buffer.mb", "128")
  val sc = new SparkContext(conf)
  val readFile = sc.textFile("hdfs://bitcluster/user/sparker/bit-data/THR_DICO_ID_REPORT").map(x=>x.split("\\|"))
  readFile.first()
  val tableName = "thr_dico_id_report"
  readFile.foreachPartition{
    x=> {
      val myConf = HBaseConfiguration.create()
      myConf.set("hbase.zookeeper.quorum", "sparker001,sparker002,sparker009")
      myConf.set("hbase.zookeeper.property.clientPort", "2181")
      myConf.set("hbase.defaults.for.version.skip", "true")
      val myTable = new HTable(myConf, TableName.valueOf(tableName))
      myTable.setAutoFlush(false, false)
      myTable.setWriteBufferSize(3*1024*1024)
      x.foreach { y => {
        println(y(0) + ":::" + y(1))
        val p = new Put(Bytes.toBytes(y(1)))
        //scan 'thr_dico_id_report',{COLUMNS => 'basic_info:P_ID_CD'}
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("P_ID_CD"), Bytes.toBytes(y(0)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("ORG_CODE"), Bytes.toBytes(y(2)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("REPORT_NO"), Bytes.toBytes(y(3)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("SUMMARY"), Bytes.toBytes(y(4)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("NAME"), Bytes.toBytes(y(5)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("SEX_CD"), Bytes.toBytes(y(6)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("SEX_CD_NAME"), Bytes.toBytes(y(7)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("SEX_CODE"), Bytes.toBytes(y(8)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("SEX_NAME"), Bytes.toBytes(y(9)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("AGE"), Bytes.toBytes(y(10)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("REPORT_TYPE_CD"), Bytes.toBytes(y(11)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("REPORT_TYPE_CD_NAME"), Bytes.toBytes(y(12)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("REPORT_TYPE_CODE"), Bytes.toBytes(y(13)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("REPORT_TYPE_DESC"), Bytes.toBytes(y(14)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("PATIENT_BELONG_CD"), Bytes.toBytes(y(28)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("ATIENT_BELONG_CD_NAME"), Bytes.toBytes(y(29)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("PATIENT_BELONG_CODE"), Bytes.toBytes(y(30)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("PATIENT_BELONG_NAME"), Bytes.toBytes(y(31)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("CAREER_CD"), Bytes.toBytes(y(32)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("CAREER_CD_NAME"), Bytes.toBytes(y(33)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("CAREER_CODE"), Bytes.toBytes(y(34)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("CAREER_NAME"), Bytes.toBytes(y(35)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("FIRST_DATE"), Bytes.toBytes(y(36)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("DISEASE_TYPE_CD"), Bytes.toBytes(y(37)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("DISEASE_TYPE_CD_NAME"), Bytes.toBytes(y(38)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("DISEASE_TYPE_CODE"), Bytes.toBytes(y(39)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("DIAG_STATUS_CD"), Bytes.toBytes(y(41)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("DIAG_STATUS_CD_NAME"), Bytes.toBytes(y(42)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("DIAG_STATUS_CODE"), Bytes.toBytes(y(43)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("DIAG_STATUS_NAME"), Bytes.toBytes(y(44)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("DIAG_DATE"), Bytes.toBytes(y(45)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("CONTAGION_TYPE_CD"), Bytes.toBytes(y(47)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("CONTAGION_TYPE_CD_NAME"), Bytes.toBytes(y(48)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("CONTAGION_TYPE_CODE"), Bytes.toBytes(y(49)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("CONTAGION_TYPE_DESC"), Bytes.toBytes(y(50)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("CONTAGION_CD"), Bytes.toBytes(y(51)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("CONTAGION_NAME_CD_NAME"), Bytes.toBytes(y(52)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("CONTAGION_CODE"), Bytes.toBytes(y(53)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("CONTAGION_NAME"), Bytes.toBytes(y(54)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("REPORT_DOC_NAME"), Bytes.toBytes(y(59)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("REPORT_ORG_NAME"), Bytes.toBytes(y(60)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("REPORT_DATE"), Bytes.toBytes(y(62)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("LAST_EDITED_TIME"), Bytes.toBytes(y(65)));
        p.add(Bytes.toBytes("basic_info"), Bytes.toBytes("ORG_NAME"), Bytes.toBytes(y(66)));

        myTable.put(p)
      }
      }
      myTable.flushCommits()
    }
  }
}
