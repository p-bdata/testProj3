import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession}

object joinTwoCsv extends App {
  System.setProperty("hadoop.home.dir", "c://winutils//")
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

  val conf = new SparkConf()
  conf.set("spark.app.name","Test LTI")
  conf.set("spark.master","local[2]")

  val spark = SparkSession.builder()
    .config(conf)
    .getOrCreate()
  val empDf = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "E:\\MyBigData\\BigData_Materials\\Weekly-Progress\\week12\\downloadables\\emp.csv")
    .load()
  val deptDf = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "E:\\MyBigData\\BigData_Materials\\Weekly-Progress\\week12\\downloadables\\dept.csv")
    .load()
  /*  empDf.createOrReplaceTempView("emp")
    deptDf.createOrReplaceTempView("dept")

    val res = spark.sql(
      """select e.ename, e.deptno, d.dname
        from emp e join dept d
        where e.deptno = d.deptno""")*/

  val res = empDf.join(deptDf, empDf("deptno").equalTo(deptDf("deptno")), "inner").selectExpr("ename", "dname")
  res.show()

  scala.io.StdIn.readLine()
  spark.stop()
}