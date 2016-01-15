import java.sql.{PreparedStatement, DriverManager}

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Administrator on 2015/12/24.
 */
class reg_count {
  val conf = new SparkConf().setAppName("reg count")
  conf.set("spark.kryoserializer.buffer.mb","128")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
  hiveContext.sql("use bitdb")

  val height="HEIGHT"
  val weight="WEIGHT"
  val bmi = "BMI"
  val left_sbp= "LEFT_SBP"
  val right_sbp= "RIGHT_SBP"
  val left_dbp = "LEFT_DBP"
  val right_dbp="RIGHT_DBP"

  val heart_rate_code = "HEART_RATE_CODE"
  val hear_murmurs_desc = "HEART_MURMURS_DESC"
  val pul_exc = "PUL_EXC"
  val abdo_tend_code = "ABDO_TEND_CODE"
  val hepa_code = "HEPA_CODE"
  val sple_code = "SPLE_CODE"

  val skin_check_name = "SKIN_CHECK_NAME"
//age
  val age1="<=20"
  val age2="21-30"
  val age3="31-40"
  val age4="41-50"
  val age5="51-60"
  val age6=">60"


  val yibansql="create table if not exists yiban as select mpi_person_id,age from TM_PUB_CHECKUP_INFO where "+height+" is not null or "+weight+" is not null or "+bmi+" is not null or "+left_sbp+" is not null or "+right_sbp+" is not null or "+left_dbp+" is not null or "+right_dbp+" is not null"
  hiveContext.sql(yibansql)
  val neikesql="create table if not exists neike as select mpi_person_id,age from TM_PUB_CHECKUP_INFO where "+heart_rate_code+" is not null or "+hear_murmurs_desc+" is not null or "+pul_exc+" is not null or "+abdo_tend_code+" is not null or "+hepa_code+" is not null or "+sple_code+" is not null"
  hiveContext.sql(neikesql)
  val waikesql="create table if not exists waike as select mpi_person_id,age from TM_PUB_CHECKUP_INFO where "+skin_check_name+" is not null"
  hiveContext.sql(waikesql)

  val ageyiban="select count(mpi_person_id),case when age <=20 then '<=20' when age BETWEEN 21 AND 30 THEN '20-30' WHEN age BETWEEN 31 AND 40 THEN '31-40' WHEN age BETWEEN 41 AND 50 THEN '41-50' WHEN age BETWEEN 51 AND 60 THEN '51-60' WHEN age >60 THEN '>60' end from yiban group  by case when age <=20 then '<=20' when age BETWEEN 21 AND 30 THEN '20-30' WHEN age BETWEEN 31 AND 40 THEN '31-40' WHEN age BETWEEN 41 AND 50 THEN '41-50' WHEN age BETWEEN 51 AND 60 THEN '51-60' WHEN age >60 THEN '>60' end"
  val ageneike="select count(mpi_person_id),case when age <=20 then '<=20' when age BETWEEN 21 AND 30 THEN '20-30' WHEN age BETWEEN 31 AND 40 THEN '31-40' WHEN age BETWEEN 41 AND 50 THEN '41-50' WHEN age BETWEEN 51 AND 60 THEN '51-60' WHEN age >60 THEN '>60' end from neike group  by case when age <=20 then '<=20' when age BETWEEN 21 AND 30 THEN '20-30' WHEN age BETWEEN 31 AND 40 THEN '31-40' WHEN age BETWEEN 41 AND 50 THEN '41-50' WHEN age BETWEEN 51 AND 60 THEN '51-60' WHEN age >60 THEN '>60' end"
  val agewaike="select count(mpi_person_id),case when age <=20 then '<=20' when age BETWEEN 21 AND 30 THEN '20-30' WHEN age BETWEEN 31 AND 40 THEN '31-40' WHEN age BETWEEN 41 AND 50 THEN '41-50' WHEN age BETWEEN 51 AND 60 THEN '51-60' WHEN age >60 THEN '>60' end from waike group  by case when age <=20 then '<=20' when age BETWEEN 21 AND 30 THEN '20-30' WHEN age BETWEEN 31 AND 40 THEN '31-40' WHEN age BETWEEN 41 AND 50 THEN '41-50' WHEN age BETWEEN 51 AND 60 THEN '51-60' WHEN age >60 THEN '>60' end"

  hiveContext.sql(ageyiban)
  hiveContext.sql(ageneike)
  hiveContext.sql(agewaike)
  val ages = Array("<=20","21-30","31-40","41-50","51-60",">60")
  Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()
  val conn = DriverManager.getConnection("jdbc:oracle:thin:@10.2.1.223:1521:CCDM", "CCDM_BIT", "123456")
  var ps: PreparedStatement = null
  val results13 = new Array[Float](300)
  val result = new Array[Int](10)
  for (i <- 0 to ages.length-1) {
      hiveContext.sql("select count(mpi_person_id),case when age <=20 then '<=20' end from yiban group  by case when age <=20 then '<=20' end")

    ps = conn.prepareStatement("insert into age_exam values(?,?,?,?)")
  //  ps.setInt(1, ("20"+years(i)).toInt)
  //  for(j <- 0 to months.length - 1){
  //    ps.setInt(j+2, results13(j).toInt)
    }
    ps.executeUpdate()

}
