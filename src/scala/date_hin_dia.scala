import java.sql.{PreparedStatement, DriverManager}

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Administrator on 2015/12/30.
 */
class date_hin_dia {
  val conf = new SparkConf().setAppName("Apply Schema")
  conf.set("spark.kryoserializer.buffer.mb", "128")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
  hiveContext.sql("use bitdb")
  val date_htn = hiveContext.sql("select count(distinct t.MPI_PERSON_ID)as num1,year(t.REGISTER_DATE) years from TM_DIMA_HTN_REG t,TM_PERSON_INFO p where t.REGISTER_DATE!=\"null\" and t.MPI_PERSON_ID=p.MPI_PERSON_ID group by year(t.REGISTER_DATE)").toDF()
  val date_dia = hiveContext.sql("select count(distinct t.MPI_PERSON_ID)as num2,year(t.REGISTER_DATE) years from TM_DIMA_DIABETES_REG t,TM_PERSON_INFO p where t.REGISTER_DATE!=\"null\" and t.MPI_PERSON_ID=p.MPI_PERSON_ID group by year(t.REGISTER_DATE)").toDF()
  val yeara = Array("2010","2011", "2012", "2013", "2014","2015")
  Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()
  val conn = DriverManager.getConnection("jdbc:oracle:thin:@10.2.1.223:1521:bitbizdb", "CCDM_BIT", "123456")
  var ps: PreparedStatement = null
  for (i <- 0 to yeara.length - 1) {
    val person_htn1=date_htn.filter(date_htn("years").===(yeara(i))).collect().apply(0).apply(0).toString.toInt
    val person_dia1=date_dia.filter(date_dia("years").===(yeara(i))).collect().apply(0).apply(0).toString.toInt
    ps = conn.prepareStatement("insert into spark_date_hin_dia values(?,?,?)")
    ps.setString(1,yeara(i))
    ps.setInt(2,person_htn1)
    ps.setInt(3,person_dia1)
    ps.executeUpdate()
  }
  conn.close()
}
