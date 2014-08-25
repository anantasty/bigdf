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
- https://github.com/AyasdiOpenSource/df/wiki/ for examples
- Look at DFTest.scala for usage [a simple example is shown below]


