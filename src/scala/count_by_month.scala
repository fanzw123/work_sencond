import java.sql.{PreparedStatement, DriverManager}

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Administrator on 2015/12/28.
 */
class count_by_month {
  val conf = new SparkConf().setAppName("Apply Schema")
  conf.set("spark.kryoserializer.buffer.mb", "128")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
  hiveContext.sql("use bitdb")
  val dates = hiveContext.sql("select count(distinct MPI_PERSON_ID),year(CHECKUP_DATE) as years,month(CHECKUP_DATE) as months from spark_checkup_info where CHECKUP_DATE!=\"null\" group by year(CHECKUP_DATE),month(CHECKUP_DATE)").toDF()
  dates.show()
  dates.groupBy(dates("years"),dates("months"))
  Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()
  val conn = DriverManager.getConnection("jdbc:oracle:thin:@10.2.1.223:1521:bitbizdb", "CCDM_BIT", "123456")
  var ps: PreparedStatement = null

}
