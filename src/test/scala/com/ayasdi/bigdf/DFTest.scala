/* Ayasdi Inc. Copyright 2014 - all rights reserved. */
/**
 * @author mohit
 *  dataframe on spark
 */

package com.ayasdi.bigdf

import scala.collection.TraversableOnce.MonadOps
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.scalatest.FunSuite
import com.ayasdi.bigdf._
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.rdd.DoubleRDDFunctions
import scala.reflect.runtime.universe._
import org.apache.spark.SparkConf

class DFTest extends FunSuite with BeforeAndAfterAll {
    var sc: SparkContext = _

    override def beforeAll {
        SparkUtil.silenceSpark
        System.clearProperty("spark.master.port")
        sc = new SparkContext("local[4]", "abcd")
    }

    override def afterAll {
        sc.stop
    }

    private def makeDF = {
        val h = Vector("a", "b", "c", "Date")
        val v = Vector(Vector(11.0, 12.0, 13.0),
            Vector(21.0, 22.0, 23.0),
            Vector(31.0, 32.0, 33.0),
            Vector(1.36074391383E12, 1.360616948975E12, 1.36055080601E12))
        DF(sc, h, v)
    }

    private def makeDFWithNAs = {
        val h = Vector("a", "b", "c", "Date")
        val v = Vector(Vector(Double.NaN, 12.0, 13.0),
            Vector("b1", "", "b3"),
            Vector(31.0, 32.0, 33.0),
            Vector(1.36074391383E12, 1.360616948975E12, 1.36055080601E12))
        DF(sc, h, v)
    }

    private def makeDFWithString = {
        val h = Vector("a", "b", "c", "Date")
        val v = Vector(Vector("11.0", "12.0", "13.0"),
            Vector(21.0, 22.0, 23.0),
            Vector(31.0, 32.0, 33.0),
            Vector(1.36074391383E12, 1.360616948975E12, 1.36055080601E12))
        DF(sc, h, v)
    }

    private def makeDFFromCSVFile(file: String) = {
        DF(sc, file, ',')
    }

    test("Construct: DF from Vector") {
        val df = makeDF
        assert(df.numCols === 4)
        assert(df.numRows === 3)
    }

    test("Construct: DF from CSV file") {
        val df = makeDFFromCSVFile("src/test/resources/pivot.csv")
        assert(df.numCols === 4)
        assert(df.numRows === 4)
    }

    test("Column Index: Refer to a column of a DF") {
        val df = makeDF
        val col = df("a")
        assert(col.tpe === typeOf[Double])
        assert(col.index === 0)
        assert(col.rdd.count === 3)
    }

    test("Column Index: Refer to non-existent column of a DF") {
        val df = makeDF
        val col = df("aa")
        assert(col === null)
    }

    test("Column Index: Refer to multiple columns of a DF") {
        val df = makeDF
        val colSeq = df("a", "b")
        val acol = colSeq.cols(0)
        val bcol = colSeq.cols(1)
        assert(acol._1 === "a")
        assert((acol._2 eq df("a")) === true)
        assert(bcol._1 === "b")
        assert((bcol._2 eq df("b")) === true)
    }

    test("Column Index: Refer to non-existent columns of a DF") {
        val df = makeDF
        val colSeq = df("a", "bb")
        assert(colSeq === null)
    }

    test("Column Index: Slices") {
        val df = makeDF
        val colSeq1 = df(0)
        assert(colSeq1.cols.length === 1)
        assert((colSeq1.cols(0)._2 eq df("a")) === true)
        val colSeq2 = df(0 to 0, 1 to 3)
        assert(colSeq2.cols.length === 4)
        assert((colSeq2.cols(0)._2 eq df("a")) === true)
        assert((colSeq2.cols(1)._2 eq df("b")) === true)
        assert((colSeq2.cols(2)._2 eq df("c")) === true)
    }

    test("Column Index: Rename") {
        val df = makeDF
        val df2 = df.rename(Map("a" -> "aa", "b" -> "bb", "cc" -> "c"))
        assert((df eq df2) === true)
        assert(df2.colIndexToName(0) === "aa")
        assert(df2.colIndexToName(1) === "bb")
        assert(df2.colIndexToName(2) === "c")

        val df3 = makeDF
        val df4 = df3.rename(Map("a" -> "aa", "b" -> "bb", "cc" -> "c"), false)
        assert((df3 ne df4) === true)
        assert(df4.colIndexToName(0) === "aa")
        assert(df4.colIndexToName(1) === "bb")
        assert(df4.colIndexToName(2) === "c")
    }

    test("Parsing: Parse doubles") {
        val df = makeDFFromCSVFile("src/test/resources/mixedDoubles.csv")
        assert(df("Feature1").parseErrors === 1)
    }

    test("Filter/Select: Double Column comparisons with Scalar") {
        val df = makeDF
        val dfEq12 = df(df("a") == 12.0)
        assert(dfEq12.numRows === 1)
        val dfNe12 = df(df("a") != 12)
        assert(dfNe12.numRows === 2)
        val dfGt12 = df(df("a") > 12)
        assert(dfGt12.numRows === 1)
        val dfGtEq12 = df(df("a") >= 12)
        assert(dfGtEq12.numRows === 2)
        val dfLt12 = df(df("a") < 12)
        assert(dfLt12.numRows === 1)
        val dfLtEq12 = df(df("a") <= 12)
        assert(dfLtEq12.numRows === 2)
    }

    test("Filter/Select: Double Column comparisons with Scalar, no match") {
        val df = makeDF
        val dfGt13 = df(df("a") == 133)
        assert(dfGt13.numRows === 0)
    }

    test("Filter/Select: String Column comparisons with Scalar") {
        val df = makeDFWithString
        val dfEq12 = df(df("a") == "12.0")
        assert(dfEq12.numRows === 1)
        val dfGt12 = df(df("a") == "12.0")
        assert(dfGt12.numRows === 1)
        val dfGtEq12 = df(df("a") >= "12.0")
        assert(dfGtEq12.numRows === 2)
        val dfLt12 = df(df("a") < "12.0")
        assert(dfLt12.numRows === 1)
        val dfLtEq12 = df(df("a") <= "12.0")
        assert(dfLtEq12.numRows === 2)
    }

    test("Filter/Select: String Column comparisons with Scalar, no match") {
        val df = makeDFWithString
        val dfGt13 = df(df("a") > "13.0")
        assert(dfGt13.numRows === 0)
    }

    test("Filter/Select: Logical combinations of predicates") {
        val df = makeDF
        val dfAeq12AndBeq22 = df(df("a") == 12.0 && df("b") == 22)
        assert(dfAeq12AndBeq22.numRows === 1)
        val dfAeq12OrBeq23 = df(df("a") == 12 || df("b") == 23)
        assert(dfAeq12OrBeq23.numRows === 2)
        val dfNotAeq12 = df(!(df("a") == 12))
        assert(dfNotAeq12.numRows === 2)
        val dfAeq12XorBeq23 = df(df("a") == 12 ^^ df("b") == 23)
        assert(dfAeq12XorBeq23.numRows === 2)
    }

    test("NA: Counting NaNs") {
        val df = makeDFWithNAs
        assert(df.countColsWithNA === 2)
        assert(df.countRowsWithNA === 2)
    }

    test("NA: Dropping rows with NaN") {
        val df = makeDFWithNAs
        assert(df.numRows === 3)
        val df2 = df.dropNA
        assert(df2.numRows === 1)
    }

    test("NA: Replacing NA with something else") {
        val df = makeDFWithNAs
        assert(df.countRowsWithNA === 2)
        df("a").fillNA(99.0)
        assert(df.countRowsWithNA === 1)
        df("b").fillNA("hi")
        assert(df.countRowsWithNA === 0)
    }

    test("Column Ops: New column as simple function of existing ones") {
        val df = makeDF
        val aa = df("a").number.first
        val bb = df("b").number.first

        df("new") = df("a") + df("b")
        assert(df("new").number.first === aa + bb)
        df("new") = df("a") - df("b")
        assert(df("new").number.first === aa - bb)
        df("new") = df("a") * df("b")
        assert(df("new").number.first === aa * bb)
        df("new") = df("a") / df("b")
        assert(df("new").number.first === aa / bb)
    }

    test("Column Ops: New column as custom function of existing ones") {
        val df = makeDF
        df("new") = df("a", "b").map(TestFunctions.summer)
        assert(df("new").number.first === 21 + 11)
    }

    test("Aggregate") {
        val df = makeDF
        df("groupByThis") = df("a").map { x => 1.0 }
        val sumOfA = df.aggregate("groupByThis", "a", AggSimple)
        assert(sumOfA.first._2 === df("a").number.sum)
        val arrOfA = df.aggregate("groupByThis", "a", AggCustom)
        assert(arrOfA.first._2 === Array(11.0, 12.0, 13.0))
    }

    test("Pivot") {
        val df = makeDFFromCSVFile("src/test/resources/pivot.csv")
        val df2 = df.pivot("Customer", "Period")
        df2.describe
        df2.list
    }
}

class DFTestWithKryo extends DFTest {
    override def beforeAll {
        SparkUtil.silenceSpark
        System.clearProperty("spark.master.port")

        var conf = new SparkConf()
            .setMaster("local[4]")
            .setAppName("DFTestWithKryo")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        sc = new SparkContext(conf)
    }
}

object AggSimple extends Aggregator[Double] {
    def aggregate(a: Double, b: Double) = a + b
}

object AggCustom extends Aggregator[Array[Double]] {
    override def convert(a: Array[Any]): Array[Double] = { Array(a(colIndex).asInstanceOf[Double]) }
    def aggregate(a: Array[Double], b: Array[Double]) = a ++ b
}

case object TestFunctions {
    def summer(cols: Array[Any]) = {
        (cols(0), cols(1)) match {
            case (a: Double, b: Double) => a + b
        }
    }
}

object SparkUtil {
    def silenceSpark {
        setLogLevels(Level.WARN, Seq("spark", "org", "akka"))
    }

    def setLogLevels(level: org.apache.log4j.Level, loggers: TraversableOnce[String]) = {
        loggers.map {
            loggerName =>
                val logger = Logger.getLogger(loggerName)
                val prevLevel = logger.getLevel()
                logger.setLevel(level)
                loggerName -> prevLevel
        }.toMap
    }
}