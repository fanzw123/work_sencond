import java.sql.{PreparedStatement, DriverManager}
import org.apache.spark.{SparkContext, SparkConf}
class reg_count1 {
  val conf = new SparkConf().setAppName("reg count")
  conf.set("spark.kryoserializer.buffer.mb","128")
  val sc = new SparkContext(conf)
  val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
  hiveContext.sql("use bitdb")
  val users = hiveContext.table("TM_PUB_CHECKUP_INFO")
  val ages = Array("0-20","21-30","31-40","41-50","51-60","61-100")
  val yearsa=Array(0,21,31,41,51,61)
  val yearsb=Array(20,30,40,50,60,100)
  val resulty = new Array[Int](10)
  val resultn = new Array[Int](10)
  val resultw = new Array[Int](10)
  Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()
  val conn = DriverManager.getConnection("jdbc:oracle:thin:@10.2.1.223:1521:bitbizdb", "CCDM_BIT", "123456")
  var ps: PreparedStatement = null
  for(i <- 0 to yearsa.length-1) {
    resulty(i)= users.filter(users("HEIGHT").!==("null") or users("WEIGHT").!==("null") or users("BMI").!==("null") or users("LEFT_SBP").!==("null") or users("RIGHT_SBP").!==("null") or users("LEFT_DBP").!==("null") or users("RIGHT_DBP").!==("null")).filter(users("age").between(yearsa(i),yearsb(i))).count().toInt
    resultn(i) =users.filter(users("HEART_RATE_CODE").!==("null") or users("HEART_MURMURS_DESC").!==("null") or users("PUL_EXC").!==("null") or users("ABDO_TEND_CODE").!==("null") or users("HEPA_CODE").!==("null") or users("SPLE_CODE").!==("null")).filter(users("age").between(yearsa(i),yearsb(i))).count().toInt
    resultw(i) = users.filter(users("SKIN_CHECK_NAME").!==("null")).filter(users("age").between(yearsa(i),yearsb(i))).count().toInt
    ps = conn.prepareStatement("insert into age_exam(NIANLING,YIBAN,NEIKE,WAIKE) values(?,?,?,?)")
    ps.setString(1,ages(i))
    ps.setInt(2,resulty(i))
    ps.setInt(3,resultn(i))
    ps.setInt(4,resultw(i))
    ps.executeUpdate()
  }
  conn.close()
}
