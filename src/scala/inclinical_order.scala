import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Administrator on 2015/12/31.
 */
class inclinical_order {
  val conf = new SparkConf().setAppName("Apply Schema")
  conf.set("spark.kryoserializer.buffer.mb", "128")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
  hiveContext.sql("use bitdb")
  val date_order = hiveContext.sql("select count(MPI_PERSON_ID)as num1,year(t.ORDER_BEGIN_DATE) years from TM_INCLINICAL_ORDER where ORDER_BEGIN_DATE!=\"null\" group by year(ORDER_BEGIN_DATE)").toDF()
  date_order.show()
}
