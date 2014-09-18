/* Ayasdi Inc. Copyright 2014 - all rights reserved. */
/**
 * @author mohit
 *  dataframe on spark
 */
package com.ayasdi.df

import scala.collection.mutable.HashMap
import scala.util.Try
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.commons.csv._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.reflect.ClassTag
import com.ayasdi.df.Preamble._
import scala.collection.JavaConversions

/**
 * types of joins
 */
object JoinType extends Enumeration {
    type JoinType = Value
    val Inner, Outer = Value
}

object ColumnType extends Enumeration {
    type ColumnType = Value
    val String, Double = Value
}

/*
 * Data Frame is a map of column key to an RDD containing that column
 * constructor is private, instances are created by factory calls(apply) in 
 * companion object
 * Rows and Columns are mutable: They can be appended, deleted, replaced
 */
case class DF private (val sc: SparkContext,
                       val cols: HashMap[String, Column[Any]], //column key -> column r, 
                       val colIndexToName: HashMap[Int, String]) { //column serial number -> column key {
    def numCols = cols.size
    def numRows = if (cols.head._2 == null) 0 else cols.head._2.rdd.count

    override def toString() = {
        "Silence is golden"
    }

    /*
     * columns are zip'd together to get rows
     */
    private def computeRows: RDD[Array[Any]] = {
        val first = cols(colIndexToName(0)).rdd
        val rest =  (1 until colIndexToName.size).toList.map { i => cols(colIndexToName(i)).rdd } 
          
        first.zip(rest)
    }
    private var rowsRddCached: RDD[Array[Any]] = null
    def rowsRdd = {
        if (rowsRddCached != null) {
            rowsRddCached
        } else {
            rowsRddCached = computeRows
            rowsRddCached
        }
    }

    /*
     * add column keys, returns number of columns added
     */
    def addHeader(header: Array[String]) = {
        var i = 0
        header.foreach { colName =>
            cols.put(colName, null)
            colIndexToName.put(i, colName)
            i += 1
        }
        i
    }
    
    /*
     * add column keys, returns number of columns added
     */
    def addHeader(header: Iterator[String]) = {
        var i = 0
        header.foreach { colName =>
            cols.put(colName, null)
            colIndexToName.put(i, colName)
            i += 1
        }
        i
    }

    /**
     * get a column identified by name
     */
    def apply(colName: String) = {
        rowsRddCached = null //FIXME: can be optimized

        val col = cols.getOrElse(colName, null)
        if (col == null) println(s"Hmm...didn't find that column ${colName}. Do you need a spellchecker?")
        col
    }

    /**
     * get multiple columns identified by name
     */
    def apply(colNames: String*) = {
        rowsRddCached = null //FIXME: can be optimized

        val selectedCols = for (colName <- colNames)
            yield (colName, cols.getOrElse(colName, null))
        if (selectedCols.exists(_._2 == null)) {
            val notFound = selectedCols.filter(_._2 == null)
            println("You sure? I don't know about these columns" + notFound.mkString(","))
            null
        } else {
            new ColumnSeq(selectedCols)
        }
    }

    /**
     * get columns with numeric index
     * FIXME: solo has to be "5 to 5" for now, should be just "5"
     */
    def apply(indexRanges: List[Range]) = {
        rowsRddCached = null //FIXME: can be optimized

        val selectedCols = for (
            indexRange <- indexRanges;
            index <- indexRange;
            if (colIndexToName(index) != null)
        ) yield (colIndexToName(index), cols.getOrElse(colIndexToName(index), null))

        new ColumnSeq(selectedCols)
    }

    private def filter(cond: Condition) = {
        rowsRdd.filter(row => cond.check(row))
    }
    /**
     * wrapper on filter to create a new DF from filtered RDD
     */
    def where(cond: Condition): DF = {
        val filteredRows = filter(cond)
        fromRows(filteredRows)
    }

    /**
     * allow omitting keyword "where"
     */
    def apply(cond: Condition) = where(cond)

    /**
     * update a column, add or replace
     */
    def update(colName: String, that: Column[Any]) = {
        val col = cols.getOrElse(colName, null)

        cols.put(colName, that)
        if (col != null) {
            println(s"Replaced Column: ${colName}")
        } else {
            println(s"New Column: ${colName} ")
            val colIndex = colIndexToName.size
            colIndexToName.put(colIndex, colName)
            that.index = colIndex
        }
        rowsRddCached = null //invalidate cached rows
        //FIXME: incremental computation of rows
    }

    /**
     * rename columns, modify same DF or make a new one
     */
    def rename(columns: Map[String, String], inPlace: Boolean = true) = {
        val df = if (inPlace == false) new DF(sc, cols.clone, colIndexToName.clone) else this

        columns.foreach {
            case (oldName, newName) =>
                val col = df.cols.remove(oldName)
                if (!col.isEmpty) {
                    val (i, n) = df.colIndexToName.find(x => x._2 == oldName).get
                    df.colIndexToName.put(i, newName)
                    df.cols.put(newName, col.get)
                    println(s"${oldName}[${i}] --> ${newName}")
                } else {
                    println(s"Wazz that? I can't find ${oldName}, skipping it")
                }
        }
        df
    }

    /**
     * number of rows that have NA(NaN or empty string)
     */
    def countRowsWithNA = {
        val x = rowsRdd.map { row => if (DFUtils.countNaN(row) > 0) 1 else 0 }
        x.reduce { _ + _ }
    }

    /**
     * number of columns that have NA(NaN or empty string)
     */
    def countColsWithNA = {
        cols.map { col => if (col._2.hasNA) 1 else 0 }.reduce { _ + _ }
    }

    /**
     * create a new DF after removing all rows that had NAs(NaNs or empty strings)
     */
    def dropNA = {
        val rows = rowsRdd.filter { row => DFUtils.countNaN(row) == 0 }
        fromRows(rows)
    }

    /*
     * augment with key
     */
    private def keyBy(colName: String) = {
        cols(colName).rdd
    }.asInstanceOf[RDD[Any]].zip(rowsRdd)

    /**
     * group by a column, uses a lot of memory. try to use aggregate(By) instead if possible
     */
    def groupBy(colName: String) = {
        keyBy(colName).groupByKey
    }

    /**
     * group by multiple columns, uses a lot of memory. try to use aggregate(By) instead if possible
     */
    def groupBy(colNames: String*) = {
        val columns = colNames.map { cols(_) }
        val k = columns.map { _.rdd }.asInstanceOf[RDD[Any]]
        val kv = k.zip(rowsRdd)

        kv.groupByKey
    }

    /**
     * aggregate one column after grouping by another
     */
    def aggregate[U: ClassTag](aggByCol: String, aggedCol: String, aggtor: Aggregator[U]) = {
        aggtor.colIndex = cols(aggedCol).index
        keyBy(aggByCol).combineByKey(aggtor.convert, aggtor.mergeValue, aggtor.mergeCombiners)
    }

    /**
     * print brief description of the DF
     */
    def describe() {
        cols.foreach {
            case (name, col) =>
                println(s"${name}:")
                println(col.toString)
        }
    }

    private def fromRows(filteredRows: RDD[Array[Any]]) = {
        val df = new DF(sc, cols.clone, colIndexToName.clone)

        val firstRowOption = Try { filteredRows.first }.toOption
        if (firstRowOption.nonEmpty) {
            val firstRow = firstRowOption.get
            for (i <- 0 until df.numCols) {
                val t = DF.getType(firstRow(i))
                val column = if (t == ColumnType.Double) {
                    val colRdd = filteredRows.map { row => row(i).asInstanceOf[Double] }
                    df.cols.put(df.colIndexToName(i), new Column[Double](colRdd, i, 0))
                } else if (t == ColumnType.String) {
                    val colRdd = filteredRows.map { row => row(i).asInstanceOf[String] }
                    df.cols.put(df.colIndexToName(i), new Column[String](colRdd, i, 0))
                } else {
                    println(s"Could not determine type of column ${colIndexToName(i)}")
                    null
                }
                println(s"Column: ${df.colIndexToName(i)} \t\t\tType: ${t}")
            }
        } else {
            for (i <- 0 until df.numCols) {
                df.cols.put(df.colIndexToName(i), null)
            }
        }
        df
    }

}

object DF {
    /**
     * create DF from a text file with given separator
     * first line of file is a header
     */
    def apply(sc: SparkContext, inFile: String, separator: Char) = {
        val df = new DF(sc, new HashMap[String, Column[Any]], new HashMap[Int, String])

        val file = sc.textFile(inFile)
        val firstLine = file.first
        val header = CSVParser.parse(firstLine, CSVFormat.DEFAULT).iterator.next
        println(s"Found ${header.size} columns")
        df.addHeader(JavaConversions.asScalaIterator(header.iterator))

        val dataLines = file.filter(_ != firstLine)

        def guessType(col: RDD[String]) = {
            val first = col.first
            if (Try { first.toDouble }.toOption == None)
                ColumnType.String
            else
                ColumnType.Double
        }

        /*
         * CSVParser is not serializable, so create one on worker
         * Use mapPartitions to do that only once per partition
         */
        val columns = for (i <- 0 until df.numCols) yield {
            dataLines.map { line => CSVParser.parse(line, CSVFormat.DEFAULT).iterator.next.get(i) }
        }

        var i = 0
        columns.foreach { col =>
            val t = guessType(columns(i))
            println(s"Column: ${df.colIndexToName(i)} \t\t\tGuessed Type: ${t}")
            if (t == ColumnType.Double)
                df.cols.put(df.colIndexToName(i), Column(col, i))
            else
                df.cols.put(df.colIndexToName(i), new Column[String](col, i, 0))
            i += 1
        }
        df
    }

    /**
     * create a DF given column names and vectors of columns(not rows)
     */
    def apply(sc: SparkContext, header: Vector[String], vec: Vector[Vector[Any]]) = {
        val df = new DF(sc, new HashMap[String, Column[Any]], new HashMap[Int, String])
        df.addHeader(header.toArray)

        var i = 0
        vec.foreach { col =>
            col(0) match {
                case c: Double =>
                    println(s"Column: ${df.colIndexToName(i)} Type: Double")
                    df.cols.put(df.colIndexToName(i),
                        new Column[Double](sc.parallelize(col.asInstanceOf[Vector[Double]]), i, 0))

                case c: String =>
                    println(s"Column: ${df.colIndexToName(i)} Type: String")
                    df.cols.put(df.colIndexToName(i),
                        new Column[String](sc.parallelize(col.asInstanceOf[Vector[String]]), i, 0))
            }
            i += 1
        }
        df
    }

    /**
     * create a DF from a ColumnSeq
     */
    def apply(sc: SparkContext, colSeq: ColumnSeq) = {
        val df = new DF(sc, new HashMap[String, Column[Any]], new HashMap[Int, String])
        val header = colSeq.cols.map { _._1 }
        val columns = colSeq.cols.map { _._2 }

        df.addHeader(header.toArray)
        var i = 0
        columns.foreach { col =>
            println(s"Column: ${df.colIndexToName(i)} Type: Double")
            df.cols.put(df.colIndexToName(i), col)
            i += 1
        }
        df
    }

    /**
     * create a DF from a Column
     */
    def apply(sc: SparkContext, name: String, col: Column[Double]) = {
        val df = new DF(sc, new HashMap[String, Column[Any]], new HashMap[Int, String])
        val i = df.addHeader(Array(name))
        df.cols.put(df.colIndexToName(i - 1), col)
        df
    }

    def joinRdd(sc: SparkContext, left: DF, right: DF, on: String, how: JoinType.JoinType = JoinType.Inner) = {
        val leftWithKey = left.cols(on).rdd.zip(left.rowsRdd)
        val rightWithKey = right.cols(on).rdd.zip(right.rowsRdd)
        leftWithKey.join(rightWithKey)
    }

    def getType(elem: Any) = {
        elem match {
            case x: Double => ColumnType.Double
            case x: String => ColumnType.String
            case _ => {
                println(s"PANIC: unsupported column type ${elem}")
                null
            }
        }
    }

    /**
     * relational-like join two DFs
     */
    def join(sc: SparkContext, left: DF, right: DF, on: String, how: JoinType.JoinType = JoinType.Inner) = {
        val df = new DF(sc, new HashMap[String, Column[Any]], new HashMap[Int, String])
        val joinedRows = joinRdd(sc, left, right, on, how)
        val firstRow = joinedRows.first

        def leftValue(row: (Any, (Array[Any], Array[Any]))) = row._2._1
        def rightValue(row: (Any, (Array[Any], Array[Any]))) = row._2._2

        /*
         * if the two DFs being joined have columns with same names, prefix them with
         * left_ and right_ in the joined DF
         */
        def joinedColumnName(name: String, start: Int) = {
            val collision = !left.cols.keys.toSet.intersect(right.cols.keys.toSet).isEmpty
            if (!collision) {
                name
            } else {
                if (start == 0) {
                    "left_" + name
                } else {
                    "right_" + name
                }
            }
        }

        /*
         * add left or right columns to the DF
         * start = 0 for left, start = left.numCols for right
         */
        def addCols(curDf: DF, start: Int, partGetter: ((Any, (Array[Any], Array[Any]))) => Array[Any]) = {
            for (joinedIndex <- start until start + curDf.numCols) {
                val origIndex = joinedIndex - start
                val newColName = joinedColumnName(curDf.colIndexToName(origIndex), start)
                val t = getType(partGetter(firstRow)(origIndex))
                if (t == ColumnType.Double) {
                    val colRdd = joinedRows.map { row => partGetter(row)(origIndex).asInstanceOf[Double] }
                    val column = new Column[Double](colRdd, joinedIndex, 0)
                    df.cols.put(newColName, column)
                } else if (t == ColumnType.String) {
                    val colRdd = joinedRows.map { row => partGetter(row)(origIndex).asInstanceOf[String] }
                    val column = new Column[String](colRdd, joinedIndex, 0)
                    df.cols.put(newColName, column)
                } else {
                    println(s"Could not determine type of column ${left.colIndexToName(origIndex)}")
                    null
                }
                println(s"Column: ${curDf.colIndexToName(origIndex)} \t\t\tType: ${t}")
                
                df.colIndexToName(joinedIndex) = newColName
            }
        }

        addCols(left, 0, leftValue)
        addCols(right, left.numCols, rightValue)

        df
    }
}