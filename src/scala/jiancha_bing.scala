import java.sql.{PreparedStatement, DriverManager}

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Administrator on 2015/12/28.
 */
class jiancha_bing {
  val conf = new SparkConf().setAppName("jiancha bing")
  conf.set("spark.kryoserializer.buffer.mb","128")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
  hiveContext.sql("use bitdb")
  val yibansql="create table if not exists yiban as select * from TM_PUB_CHECKUP_INFO where HEIGHT!=\"null\" or WEIGHT!=\"null\" or BMI!=\"null\" or LEFT_SBP!=\"null\" or RIGHT_SBP!=\"null\" or LEFT_DBP!=\"null\" or RIGHT_DBP!=\"null\""
  val waikesql="create table if not exists waike as select mpi_person_id,age from TM_PUB_CHECKUP_INFO where HEART_RATE_CODE!=\"null\" or HEART_MURMURS_DESC!=\"null\" or PUL_EXC!=\"null\" or ABDO_TEND_CODE!=\"null\" or HEPA_CODE!=\"null\" or SPLE_CODE!=\"null\""
  val neikesql="create table if not exists neike as select mpi_person_id,age from TM_PUB_CHECKUP_INFO where SKIN_CHECK_NAME!=\"null\""
  hiveContext.sql(yibansql)
  hiveContext.sql(neikesql)
  hiveContext.sql(waikesql)
  val htnsql="select count(*) from tm_dima_htn_reg t,yiban y where t.mpi_person_id=y.mpi_person_id"
  val htn1sql="select count(*) from tm_dima_htn_reg t,waike w where t.mpi_person_id=w.mpi_person_id"
  val htn2sql="select count(*) from tm_dima_htn_reg t,neike n where t.mpi_person_id=n.mpi_person_id"
  val diabetessql="select count(*) from TM_DIMA_DIABETES_REG t,yiban y where t.mpi_person_id=y.mpi_person_id"
  val diabetes1sql="select count(*) from TM_DIMA_DIABETES_REG t,waike w where t.mpi_person_id=w.mpi_person_id"
  val diabetes2sql="select count(*) from TM_DIMA_DIABETES_REG t,neike n where t.mpi_person_id=n.mpi_person_id"
  val yibanh=hiveContext.sql(htnsql).collect().apply(0)
  val waikeh=hiveContext.sql(htn1sql).collect().apply(0)
  val neikeh=hiveContext.sql(htn2sql).collect().apply(0)
  val yiband=hiveContext.sql(diabetessql).collect().apply(0)
  val waiked=hiveContext.sql(diabetes1sql).collect().apply(0)
  val neiked=hiveContext.sql(diabetes2sql).collect().apply(0)

  Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()
  val conn = DriverManager.getConnection("jdbc:oracle:thin:@10.2.1.223:1521:bitbizdb", "CCDM_BIT", "123456")
  var ps: PreparedStatement = null
  ps = conn.prepareStatement("update jiancha_bing set NEIKE=neikeh,WAIKE=waikeh where BING='¸ßÑªÑ¹'")
  ps = conn.prepareStatement("update jiancha_bing set NEIKE=neiked,WAIKE=waiked where BING='ÌÇÄò²¡'")
  ps.executeUpdate()
  conn.close()
}
