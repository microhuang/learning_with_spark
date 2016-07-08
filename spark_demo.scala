//快速记录比对
import org.apache.spark.{ SparkContext, SparkConf }  
import java.sql.DriverManager  

object HandleGroup extends App{  
  
  val beginTime = System.currentTimeMillis()  
  //引用spark  
  val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")  
  val sc = new SparkContext(conf)  
  
  //获取文件  
  val txt1 = sc.textFile("file:///tmp/fg1.txt").map(line =>line.split("\\|"))  //1|2|3|4|5|6 \n 1|2|3|4|5|6
  val txt2 = sc.textFile("file:///tmp/fg2.txt").map(line =>line.split("\\|"))  //1|2|3|4|5|6 \n 11|2|3|4|5|6
  //txt1.take(10).foreach(_.foreach(println))
  //txt2.take(10).foreach(_.foreach(println))   //.collect()
  
  val results = txt1.map{ v =>(v(0)+v(1)+v(5),v) }.   //126,((1,2,3,4,5,6),(1,2,3,4,5,6))
    cogroup( txt2.map{v =>(v(0)+v(1)+v(5),v)} ).      //126,((1,2,3,4,5,6),(1,2,3,4,5,6)),(1,2,3,4,5,6)  1126,((),(1,2,3,4,5,6))
    filter{ case (k, (v1, v2)) => !(v1.nonEmpty && v2.nonEmpty) }  //过滤出只在一个集合出现的记录。

  /*
  val results = txt1.map{ v =>(v(0)+v(1)+v(5),v) }.   //126,((1,2,3,4,5,6),(1,2,3,4,5,6))
    cogroup( txt2.map{v =>(v(0)+v(1)+v(5),v)} ).      //126,((1,2,3,4,5,6),(1,2,3,4,5,6)),(1,2,3,4,5,6)  1126,((),(1,2,3,4,5,6))
    filter{ case (k, (v1, v2)) => !(v1.nonEmpty && v2.nonEmpty && List(v1).size==1 && List(v2).size==1 && List(v1)(0)(0)=="1" ) }
  */
  
  //JDBC预处理  
  val driver = "com.mysql.jdbc.Driver"  
  val url = "jdbc:mysql://localhost:3306/test_db"  
  val username = ""  
  val password = ""  
  Class.forName(driver)  
  val connection = DriverManager.getConnection(url, username, password)  
  val statement = connection.createStatement()  
  
  //执行sql语句  
  def runSql(v1:String,v2:String,v3:String): Unit =  
    statement.addBatch("insert into nidaye( v1, v2, v3) values('"+v1+"','"+v2+"','"+v3+"')")  //create table nidaye (v1 varchar(10), v2 varchar(10), v3 varchar(10));
  
  //遍历结果集  
  results.foreach{case (k,(v1,v2)) =>  
    v1.foreach(a =>runSql(a(0),a(1),a(5)))  
    v2.foreach(a => runSql(a(0),a(1),a(5)))  
  }  
  
  //提交  
  val resultSet = statement.executeBatch()  
  connection.close()  
  println(System.currentTimeMillis() - beginTime)  

}  
