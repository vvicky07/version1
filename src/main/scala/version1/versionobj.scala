package version1
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
// import spray.json._
// import DefaultJsonProtocol._
import java.nio.file.Files
import java.nio.file.Paths
import scala.io.Source
import java.sql.Connection
import java.sql.DriverManager

object versionobj {

   def addcolumnIndex(session:SparkSession,df : DataFrame)={
    session.sqlContext.createDataFrame(
     df.rdd.zipWithIndex.map{
       case (row,index) => Row.fromSeq(row.toSeq:+ index)
     },    
     StructType(df.schema.fields :+ StructField("incid",LongType,false))
    )
    
  }
  
  def main(args : Array[String]) : Unit = {
    
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    
    import spark.implicits._
    
    
    
   val df1 = spark.read.format("json").load("file:///C:/data/project3data/day1.json")
    df1.show()
    df1.printSchema()
    
    val dfflat1 = df1.withColumn("products",explode(col("products")))
                   .select("address.*","custid","products")
                   .withColumn("deleteind",lit(0))
                   .withColumn("batchind",lit(0))
                   
    
    dfflat1.show()
    
    val indexdf1 = addcolumnIndex(spark,dfflat1)
    indexdf1.show()
    
    indexdf1.write.format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/sqlconnection?autoReconnect=true&useSSL=false")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "custtab")
  .option("user", "root")
  .option("password", "cloudera")
  .mode("append")
  .save()
  
 

    
//// ---- DAY 2 ----- ///////
  
  val df2 = spark.read.format("json")
  .load("file:///C:/data/project3data/day2.json")
  
  
  df2.show()
  
 
  
  val flattendf2 = df2
  .withColumn("products",explode(col("products")))
  .select("address.*","custid","products")
  
  
  
  println("===========flatten data================")
  flattendf2.show()
  
  
  
  val deleteinc2 = flattendf2.withColumn("deleteind", lit(0))
  
  println("===========deleteinc data================")
  deleteinc2.show()
  
  
  val lastdata2 = spark.read.format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/sqlconnection?autoReconnect=true&useSSL=false")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("query", "select * from custtab where batchind=(select max(batchind) from custtab) and deleteind=0")
  .option("user", "root")
  .option("password", "cloudera")
  .load()
  
  println("======yesterday  data======")
  
  lastdata2.show()
  
  
  
  println("========join data===========")
  
  
  val todaycust2= df2.select("custid")
  
  
  val joindf2 = lastdata2.join(todaycust2,Seq("custid"),"left_anti")
                .withColumn("deleteind", lit(1))
                .drop("incid","batchind")
                .select("permanentAddress","temporaryAddress","custid","products","deleteind")
                
                
  joindf2.show()
  
  println("=======union df================")
  
  
  val uniondf2 = joindf2.union(deleteinc2)
  
  
  uniondf2.show()
  
  
  println("============add index column===========")
  
  
  val indexdf2 = addcolumnIndex(spark, uniondf2)
  
  indexdf2.show()
  
  
  
  
  
  
  
  val maxinc2 = lastdata2.selectExpr("max(incid)")
  maxinc2.show()
  
  val maxbatch2 = lastdata2.selectExpr("max(batchind)")
  maxbatch2.show()
  
  
  val maxincint2= maxinc2.rdd.map( x => x.mkString(""))

.collect().mkString("").toInt
  
  println("max inc id--->"+maxincint2)
  
  val maxbatchint2= maxbatch2.rdd.map( x => x.mkString(""))
                 .collect().mkString("").toInt
  
  println("max inc id--->"+maxbatchint2)
  
  
  
  
 //  maxincint  ///// ----> max incremental id
  
  
  // maxbatchint ///// ----> max batch id
  
  
  
  
  val finaldf2 = indexdf2.withColumn("incid",col("incid")+maxincint2+1)
                  .withColumn("batchind",lit(maxbatchint2+1))
  
  
  
  
  finaldf2.show()
  
  
  finaldf2.write.format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/sqlconnection?autoReconnect=true&useSSL=false")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "custtab")
  .option("user", "root")
  .option("password", "cloudera")
  .mode("append")
  .save()
  
  
  finaldf2.write.format("parquet").save("user/cloudera/custtab")
  
  
}







}
  


