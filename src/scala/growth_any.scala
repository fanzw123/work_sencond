import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Administrator on 2016/1/4.
 */
class growth_any {
  val conf = new SparkConf().setAppName("Growth Anysis")
  conf.set("spark.kryoserializer.buffer.mb", "128")
  conf.set("spark.driver.maxResultSize","4G")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
  hiveContext.sql("use bitdb")
  val table_stock = hiveContext.table("htn_growth_stock").toDF()
  val table_total = hiveContext.table("htn_growth_total").toDF()
  val table_incremental = hiveContext.table("htn_growth_incremental").toDF()
  var stock=table_stock.groupBy("REGISTER_DATE").count()
  val incremental=table_incremental.groupBy("REGISTER_DATE").count()
  stock.registerTempTable("htn1")
  incremental.registerTempTable("htn2")
  var htn11 = hiveContext.sql("select htn1.REGISTER_DATE,(htn1.count+htn2.count) as num from htn1,htn2 where htn1.REGISTER_DATE=htn2.REGISTER_DATE").toDF()
  htn11.show()
  stock=htn11
  val total=table_total.groupBy("REGISTER_DATE").count()
  total.show()

  var Es=hiveContext.sql("select avg(sbp)from htn_growth_stock").toDF().first().getDouble(0)
  val Ei=hiveContext.sql("select avg(sbp)from htn_growth_incremental").toDF().first().getDouble(0)
  val E=((m*Es)+(n*Ei))/(m+n)
  val Et=hiveContext.sql("select avg(sbp)from htn_growth_total").toDF().first().getDouble(0)
  Es=E

  val st = hiveContext.sql("select sbp from htn_growth_stock").count()
  val in =hiveContext.sql("select sbp from htn_growth_incremental").count()
  val to = hiveContext.sql("select sbp from htn_growth_total").count()

  val m=125862232
  val n=213688
  //var Es=134.17389371420015
  var fs = hiveContext.sql("select sbp from htn_growth_stock").toDF().collect()

  var sum = 0.0
  var sj = 0.0
  var s = 0.0
  for(num <- 0 to m-1){
    sj = fs.apply(num).apply(0).toString.toDouble
    s = (sj - Es)*(sj - Es)
    sum = sum + s
  }
  println(sum)
  sum =sum/m

  val fi = hiveContext.sql("select sbp from htn_growth_incremental").toDF().collect()
  var sumi = 0.0
  var ij = 0.0
  var si = 0.0
  for(num <- 0 to n-1){
   ij = fi.apply(num).apply(0).toString.toDouble
   si =(ij - Ei)*(ij - Ei)
    sumi =sumi+si
  }
  sumi=sumi/n

  val ft=(m*(sum+((Et-Es)*(Et-Es)))+n*(sumi+((Et-Ei)*(Et-Ei))))/(m+n)

  val ftt=hiveContext.sql("select sbp from htn_growth_total").toDF().collect()
  var sumt=0.0
  val mn=m+n
  var ttj=0.0
  var t=0.0
  for(num <- 0 to mn-1){
    ttj = ftt.apply(num).apply(0).toString.toDouble
    t=(ttj-Et)*(ttj-Et)
    sumt=sumt+t
  }
  println(sumt)
  val ss=sumt/mn
  // var m=hiveContext.table("htn_growth_stock").toDF().count().toInt
  // var n=hiveContext.table("htn_growth_incremental").toDF().count().toInt
  hiveContext.sql("insert into table htn_growth_stock select id,MPI_PERSON_ID,ORG_CODE,PERSON_ID_CD,PERSON_ID,FILES_SNO,REGISTER_DATE,MANAGE_LAVEL,RISK_STRATIFY,SBP,DBP,HIGHTBLOOD_COMPL,KNOW_FLAG,END_DATE,END_REASION,CONFIRM_DATE,CONFIRM_ORG_CODE from htn_growth_incremental")
 // m=hiveContext.table("htn_growth_stock").count().toInt
  hiveContext.sql("insert into table htn_growth_total select id,MPI_PERSON_ID,ORG_CODE,PERSON_ID_CD,PERSON_ID,FILES_SNO,REGISTER_DATE,MANAGE_LAVEL,RISK_STRATIFY,SBP,DBP,HIGHTBLOOD_COMPL,KNOW_FLAG,END_DATE,END_REASION,CONFIRM_DATE,CONFIRM_ORG_CODE from htn_growth_stock")
  hiveContext.sql("insert into table htn_growth_total select id+"+m+",MPI_PERSON_ID,ORG_CODE,PERSON_ID_CD,PERSON_ID,FILES_SNO,REGISTER_DATE,MANAGE_LAVEL,RISK_STRATIFY,SBP,DBP,HIGHTBLOOD_COMPL,KNOW_FLAG,END_DATE,END_REASION,CONFIRM_DATE,CONFIRM_ORG_CODE from htn_growth_stock where id<2000000")
  hiveContext.sql("insert into table htn_growth_incremental select id+"+m+",MPI_PERSON_ID,ORG_CODE,PERSON_ID_CD,PERSON_ID,FILES_SNO,REGISTER_DATE,MANAGE_LAVEL,RISK_STRATIFY,SBP,DBP,HIGHTBLOOD_COMPL,KNOW_FLAG,END_DATE,END_REASION,CONFIRM_DATE,CONFIRM_ORG_CODE from htn_growth_stock where id<2000000")
 // n=hiveContext.table("htn_growth_incremental").count().toInt





  var stock_data = hiveContext.sql("select sbp from htn_growth_stock").toDF()
  (stock_data("sbp").-(Es)).*(stock_data("sbp").-(Es))
  stock_data.show()










}
