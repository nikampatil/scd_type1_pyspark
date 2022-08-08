import pyspark

from pyspark.sql import SparkSession

spark=SparkSession.builder.appName('spark_scd_type_1').getOrCreate()

emp_src=spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/swapnil").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "emp_spark").option("user", "root").option("password", "root").load()
emp_src.show()

emp_target=spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/swapnil").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "emp_spark_scd1").option("user", "root").option("password", "root").load()



emp_src=emp_src.withColumnRenamed('empno','empno_src').withColumnRenamed('empname','empname_src').withColumnRenamed('deptno','deptno_src').withColumnRenamed('sal','sal_src')



emp_target=emp_target.withColumnRenamed('empno','empno_target').withColumnRenamed('sal','sal_target')



emp_scd=emp_src.join(emp_target,emp_src.empno_src==emp_target.empno_target,how='left')

emp_scd.show()

#flag the record for insert and update

from pyspark.sql.functions import lit

from pyspark.sql import functions as f

scd_df=emp_scd.withColumn('INS_FLAG',f.when((emp_scd.empno_src!=emp_scd.empno_target)| emp_scd.empno_target.isNull(),'Y').otherwise('NA'))

scd_df.show()
#insert record into target.
#renaming columns as per table column names.
emp_ins=scd_df.select(scd_df['empno_src'].alias('empno'),scd_df['empname_src'].alias('empname'),scd_df['deptno_src'].alias('deptno'),scd_df['sal_src'].alias('sal'))

emp_ins.write.format("jdbc").mode('append').option("url", "jdbc:mysql://localhost:3306/swapnil").option("driver","com.mysql.jdbc.Driver").option("dbtable", "emp_spark_scd1").option("user", "root").option("password", "root").save()



