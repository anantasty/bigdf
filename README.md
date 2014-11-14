# What is this?
DataFrames are useful constructs for data scientists, popularized by [R] and [pandas].

[pandas]:http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.html
[R]:http://www.r-tutor.com/r-introduction/data-frame
[https://github.com/AyasdiOpenSource/spark]:https://github.com/AyasdiOpenSource/spark
Implementations of these packages are not natively distributed - their DataFrames struggle with big data. bigdf is a dataframe on top of Apache Spark. It is written as an internal Scala DSL with the look and feel of Pandas to make it usable for those familiar with Pandas to use it.

A gentle introduction is available at:
> http://www.slideshare.net/codeninja4086/df-38948475?utm_source=slideshow03&utm_medium=ssemail&utm_campaign=iupload_share_slideshow


### How to get it
- Choose a repos directory (make one if it doesn't exist)
```sh
export $REPOS = ~/Documents/repos
```
- Install maven, scala and sbt (brew instructions below)
```sh
$ brew install maven
$ brew install scala
$ brew install sbt
```
- Get patched Spark from [https://github.com/AyasdiOpenSource/spark] [A minor patch is needed on Spark to make bigdf more efficient. A pull request has been submitted and when it is merged you can use regular Spark distribution] 
```sh
$ cd $REPOS
$ git clone https://github.com/AyasdiOpenSource/spark.git
```
- Build Spark using the following commands
```sh
$ cd spark
$ export MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"
$ mvn -Dhadoop.version=2.0.0-mr1-cdh4.2.0 \
-DskipTests clean package
```
- clone the bigdf repo
```sh
$ cd $REPOS
$ git clone git@bitbucket.org:ayasdi/bigdf.git
```
- copy spark assembly jar(after bulding it) to bigdf/lib
```sh
$ cp spark/assembly/target/scala-2.xx/spark-assembly-1.2.0-SNAPSHOT-hadoop2.0.0-mr1-cdh4.2.0.jar bigdf/lib
```
- sbt update, package, and test(optional) in the bigdf directory.
```sh
$ cd bigdf
$ sbt update
$ sbt package
$ sbt test
```
- Start a shell and begin playing.
```sh
$ cd $REPOS
$ ./spark/bin/spark-shell --jars ./bigdf/target/scala-2.10/bigdf_xxxx.jar 
```
- https://github.com/AyasdiOpenSource/bigdf/wiki/ for examples
- Look at DFTest.scala for usage [a simple example is shown below]