What is this?
=============
Dataframe is a very useful construct for data scientists.

http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.html

http://www.r-tutor.com/r-introduction/data-frame

R and Pandas dataframes cannot handle big data because they are not distributed. bigdf is a dataframe on top of Apache Spark that can handle big data. It is written as an internal Scala DSL with the look and feel of Pandas to make it easier for those familiar with Pandas to use it.

A gentle introduction is here:
http://www.slideshare.net/codeninja4086/df-38948475?utm_source=slideshow03&utm_medium=ssemail&utm_campaign=iupload_share_slideshow


How to get it?
==============
- Get Spark from https://github.com/AyasdiOpenSource/spark [A minor patch is needed on Spark to make bigdf more efficient. A pull request has been submitted and when it is merged you can use regular Spark distribution] 
- clone the bigdf repo
- copy spark assembly jar(after bulding it) to bigdf/lib
- sbt update
- sbt package
- sbt test, if you want to run some unit tests. you can skip this step
- /path/to/spark/bin/spark-shell --jars /path/to/bigdf/target/scala-2.10/bigdf_2.10-0.1.jar 
- https://github.com/AyasdiOpenSource/bigdf/wiki/ for examples
- Look at DFTest.scala for usage [a simple example is shown below]


