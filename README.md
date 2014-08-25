What is this?
=============
A lot of data scientists use the python library pandas for quick exploration of data. The most useful construct in pandas (based on R, I think) is the dataframe, which is a 2D array(aka matrix) with the option to “name” the columns (and rows). But pandas is not distributed, so there is a limit on the data size that can be explored.
Spark is a great map-reduce like framework that can handle very big data by using a shared nothing cluster of machines.
This work is an attempt to provide a pandas-like DSL on top of spark, so that data scientists familiar with pandas have a very gradual learning curve.

How does it work?
=================
A dataframe contains a collection of columns that are indexed by “names” given to them and also by an integer. 
The “IDE” is the Scala REPL. Because Scala has some nice constructs that enable creating (internal)DSLs, no parser and/or interpreter is needed. An added benefit is that constructs not supported by the DSL can be written in Scala in the same environment and DSL and non-DSL code can be mixed arbitrarily.

How to get it?
==============
- Get Spark from https://spark.apache.org/
- clone this repo
- copy spark assembly jar to df/lib [you may add an SBT dependency but you have to make sure your spark cluster runs the same spark version as the one you are linking df to]
- sbt update
- sbt package
- sbt test, if you want to run some unit tests. you can skip this step
- /path/to/spark/bin/spark-shell --jars /path/to/df/target/scala-2.10/df_2.10-0.1.jar 
- Look at DFTest.scala for usage [a simple example is shown below]


Using Scala version 2.10.4 (Java HotSpot(TM) 64-Bit Server VM, Java 1.7.0_40)
Type in expressions to have them evaluated.
Type :help for more information.
14/08/04 10:18:38 WARN SparkConf: 

<<<deleted logs>>>

14/08/04 10:18:39 INFO SparkILoop: Created spark context..
Spark context available as sc.

<<<deleted logs>>>

scala> import org.apache.log4j.Level

<<<silence most logs>>>
scala> import org.apache.log4j.Level
import org.apache.log4j.Level

scala> import org.apache.log4j.Logger
import org.apache.log4j.Logger

scala> Logger.getLogger("org").setLevel(Level.WARN)

scala> Logger.getLogger("akka").setLevel(Level.WARN)

scala> import com.ayasdi.df._
import com.ayasdi.df._

scala> import com.ayasdi.df._
import com.ayasdi.df._

scala> val h = Vector("a", "b", "c", "Date")
h: scala.collection.immutable.Vector[String] = Vector(a, b, c, Date)

scala> val v = Vector(Vector(11.0, 12.0, 13.0), Vector(21.0, 22.0, 23.0), Vector(31.0, 32.0, 33.0), Vector(1.36074391383E12, 1.360616948975E12, 1.36055080601E12))
v: scala.collection.immutable.Vector[scala.collection.immutable.Vector[Double]] = Vector(Vector(11.0, 12.0, 13.0), Vector(21.0, 22.0, 23.0), Vector(31.0, 32.0, 33.0), Vector(1.36074391383E12, 1.360616948975E12, 1.36055080601E12))

scala> val df2 = DF(sc, h, v)
Column: a Type: Double
Column: b Type: Double
Column: c Type: Double
Column: Date Type: Double
df2: com.ayasdi.df.DF = Silence is golden

scala> val c = df2("a") + df2("b")
c: com.ayasdi.df.Column[Any] = 
	type:Double
	count:3
	parseErrors:0

scala> df2("a+b") = c
New Column: a+b 

scala> df2.describe
b:
	type:Double
	count:3
	parseErrors:0
Date:
	type:Double
	count:3
	parseErrors:0
a+b:
	type:Double
	count:3
	parseErrors:0
a:
	type:Double
	count:3
	parseErrors:0
c:
	type:Double
	count:3
	parseErrors:0


