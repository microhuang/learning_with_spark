import sys

from pyspark import SparkContext, SparkConf

from pyspark.sql import SQLContext
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, Row

if __name__ == "__main__":
    conf = SparkConf().setAppName("mysql_read_write_duizhang").setMaster("local[*]")
    sc = SparkContext(conf=conf, pyFiles=[])

    sqlContext = SQLContext(sc)

    #从文件加载比对数据
    txt1 = sc.textFile("file:///tmp/fg1.txt").map(lambda line:line.split("|"))
    txt2 = sc.textFile("file:///tmp/fg2.txt").map(lambda line:line.split("|"))

    url = "jdbc:mysql://localhost:3306/test_db"
    table = "nidaye"
    properties = {"user": "", "password": ""}

    '''
    #从mysql加载数据
    #df = sqlContext.read.jdbc(url=url, table=table, properties=properties)
    df = sqlContext.read.format("jdbc").options(url=url+"?user="+properties['user']+"&password="+properties['password'], dbtable=table, driver="com.mysql.jdbc.Driver").load()
    df.printSchema()

    results = df.select("v1", "v2", "v3")
    res = results.map(lambda p: "v1: " + p.v1 + " v2: " + p.v2 + " v3: " + p.v3)
    for l in res.collect():
        print(l)
    print(res)
    '''

    #过滤规则：反 －－ 记录在双方都出现、且只出现了1次、且双方2/3/4字段值相等
    results = txt1.map( lambda v:(v[0]+v[1]+v[5],v)).cogroup(txt2.map(lambda v:(v[0]+v[1]+v[5],v))).filter(lambda l:not (l[1][0] and l[1][1] and len(l[1][0])==1 and len(l[1][1])==1 and list(l[1][0])[0][2]==list(l[1][1])[0][2] and list(l[1][0])[0][3]==list(l[1][1])[0][3] and list(l[1][0])[0][4]==list(l[1][1])[0][4]))
    #results = results.collect()
    #print(results)

    #写入mysql
    #准备记录
    sql_res = results.map(lambda L:(tuple(L[1][0])+tuple(L[1][1]))[0])
    sql_res = sql_res.map(lambda L:(L[2],L[3],L[4])).take(10)            #筛选入库字段
    sql_rdd = sc.parallelize(sql_res)
    #匹配schema
    schema = StructType([StructField("v1", StringType(), False),
                         StructField("v2", StringType(), False),
                         StructField("v3", StringType(), False)])
    df = sqlContext.createDataFrame(sql_rdd, schema)
    df.write.mode("overwrite").jdbc(url=url+"?user="+properties['user']+"&password="+properties['password'], table=table)

    sc.stop()
