/* Ayasdi Inc. Copyright 2014 - all rights reserved. */
/**
 * @author mohit
 *         big dataframe on spark
 */
package com.ayasdi.bigdf
import org.apache.commons.csv._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel.MEMORY_ONLY_SER

import scala.collection.JavaConversions
import scala.collection.mutable.HashMap
import scala.reflect.runtime.{universe => ru}
import scala.reflect.{ClassTag, classTag}
import scala.util.{Random, Try}

/**
 * types of joins
 */
object JoinType extends Enumeration {
  type JoinType = Value
  val Inner, Outer = Value
}

/**
 * Data Frame is a map of column key to an RDD containing that column
 * constructor is private, instances are created by factory calls(apply) in 
 * companion object
 * Number of rows cannot change. Columns can be added, removed, mutated
 */
case class DF private(val sc: SparkContext,
                      val cols: HashMap[String, Column[Any]] = new HashMap[String, Column[Any]],
                      val colIndexToName: HashMap[Int, String] = new HashMap[Int, String],
                      val name: String) {
  /**
   * number of rows in df
   * @return number of rows
   */
  lazy val numRows = if (null == cols.head._2) 0 else cols.head._2.rdd.count
  /**
   * for large number of columns, column based filtering is faster. it is the default. try changing this
   * to true for DF with few columns
   */
  var filterWithRowStrategy = false
  var aggWithRowStrategy = false
  /**
   * default rdd caching storage level
   */
  var defaultStorageLevel: StorageLevel = MEMORY_ONLY_SER
  private var rowsRddCached: RDD[Array[Any]] = null

  def colNames = {
    (0 until numCols).map { colIndex => colIndexToName(colIndex)}.toArray
  }

  override def toString() = {
    "Silence is golden" //otherwise prints too much stuff
  }

  /**
   * save the DF to a text file
   * @param file save DF in this file
   * @param separator use this separator, default is comma
   */
  def toCSV(file: String, separator: String = ",") = {
    rowsRdd.map { row => row.mkString(separator)}.saveAsTextFile(file)
  }

  /**
   * get a column identified by name
   * @param colName name of the column
   */
  def apply(colName: String) = column(colName)

  /**
   * get multiple columns by name, indices or index ranges
   * e.g. myDF("x", "y")
   * or   myDF(0, 1, 6)
   * or   myDF(0 to 0, 4 to 10, 6 to 1000)
   * @param items Sequence of names, indices or ranges. No mix n match yet
   */
  def apply[T: ru.TypeTag](items: T*): ColumnSeq = {
    val tpe = ru.typeOf[T]
    if (tpe =:= ru.typeOf[Int])
      columnsByIndices(items.asInstanceOf[Seq[Int]])
    else if (tpe =:= ru.typeOf[String])
      columnsByNames(items.asInstanceOf[Seq[String]])
    else if (tpe =:= ru.typeOf[Range] || tpe =:= ru.typeOf[Range.Inclusive])
      columnsByRanges(items.asInstanceOf[Seq[Range]])
    else {
      println("got " + tpe); null
    }
  }

  /**
   * get multiple columns identified by names
   * @param colNames names of columns
   */
  def columnsByNames(colNames: Seq[String]) = {
    val selectedCols = for (colName <- colNames)
    yield (colName, cols.getOrElse(colName, null))
    if (selectedCols.exists(null == _._2)) {
      val notFound = selectedCols.filter(null == _._2)
      println("You sure? I don't know about these columns" + notFound.mkString(","))
      null
    } else {
      new ColumnSeq(selectedCols)
    }
  }

  /**
   * get columns by ranges of numeric indices
   * FIXME: solo has to be "5 to 5" for now, should be just "5"
   * @param indexRanges sequence of ranges like List(1 to 5, 13 to 15)
   */
  def columnsByRanges(indexRanges: Seq[Range]) = {
    val selectedCols = for (
      indexRange <- indexRanges;
      index <- indexRange;
      if (colIndexToName(index) != null)
    ) yield (colIndexToName(index), cols.getOrElse(colIndexToName(index), null))

    new ColumnSeq(selectedCols)
  }

  /**
   * get columns by sequence of numeric indices
   * @param indices Sequence of indices like List(1, 3, 13)
   */
  def columnsByIndices(indices: Seq[Int]) = {
    val indexRanges = indices.map { i => i to i}
    columnsByRanges(indexRanges)
  }

  /**
   * allow omitting keyword "where"
   */
  def apply(cond: Predicate) = where(cond)

  /**
   * wrapper on filter to create a new DF from filtered RDD
   * @param cond a predicate to filter on e.g. df("price") > 10
   */
  def where(cond: Predicate): DF = {
    if (filterWithRowStrategy) {
      fromRows(filterRowStrategy(cond))
    } else {
      DF(this, filterColumnStrategy(cond))
    }
  }

  /*
   * usually more efficient if there are a few columns
   */
  private def filterRowStrategy(cond: Predicate) = {
    rowsRdd.filter(row => cond.checkWithRowStrategy(row))
  }

  /*
   * more efficient if there are a lot of columns
   */
  private def filterColumnStrategy(cond: Predicate) = {
    val zippedColRdd = ColumnZipper(this, cond.colSeq)
    val colMap = new HashMap[Int, Int] //FIXME: switch to a fast map here
    var i = 0
    cond.colSeq.foreach { colIndex =>
      colMap(colIndex) = i
      i += 1
    }
    zippedColRdd.map(cols => cond.checkWithColStrategy(cols, colMap))
  }

  private def fromRows(filteredRows: RDD[Array[Any]]) = {
    val df = new DF(sc, cols.clone, colIndexToName.clone, name + "-fromRows")

    val firstRowOption = Try {
      filteredRows.first
    }.toOption
    if (firstRowOption.nonEmpty) {
      val firstRow = firstRowOption.get
      for (i <- 0 until df.numCols) {
        val t = DF.getType(firstRow(i))
        val column = if (t == ru.typeOf[Double]) {
          val colRdd = filteredRows.map { row => row(i).asInstanceOf[Double]}
          df.cols.put(df.colIndexToName(i), Column(sc, colRdd, i))
        } else if (t == ru.typeOf[String]) {
          val colRdd = filteredRows.map { row => row(i).asInstanceOf[String]}
          df.cols.put(df.colIndexToName(i), Column(sc, colRdd, i))
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

  /**
   * rename columns, modify same DF or make a new one
   */
  def rename(columns: Map[String, String], inPlace: Boolean = true) = {
    val df = if (inPlace == false) new DF(sc, cols.clone, colIndexToName.clone, name) else this

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
    rowsRddCached = null //fillNA could have mutated columns, recalculate rows
    val x = rowsRdd.map { row => if (CountHelper.countNaN(row) > 0) 1 else 0}
    x.reduce {
      _ + _
    }
  }

  /**
   * number of columns that have NA(NaN or empty string)
   */
  def countColsWithNA = {
    cols.map { col => if (col._2.hasNA) 1 else 0}.reduce {
      _ + _
    }
  }

  /**
   * create a new DF after removing all rows that had NAs(NaNs or empty strings)
   */
  def dropNA = {
    val rows = rowsRdd.filter { row => CountHelper.countNaN(row) == 0}
    fromRows(rows)
  }

  /**
   * group by multiple columns, uses a lot of memory. try to use aggregate(By) instead if possible
   */
  def groupBy(colNames: String*) = {
    keyBy(colNames, rowsRdd).groupByKey
  }

  def rowsRdd = {
    if (rowsRddCached != null) {
      rowsRddCached
    } else {
      rowsRddCached = computeRows
      rowsRddCached.setName(s"${name}").persist(defaultStorageLevel)
    }
  }

  /*
   * columns are zip'd together to get rows, expensive operation
   */
  private def computeRows: RDD[Array[Any]] = {
    ColumnZipper(this, (0 until numCols).toList)
  }

  /**
   * number of columns in df
   * @return number of columns
   */
  def numCols = cols.size

  /*
   * augment with key, resulting RDD is a pair with k=array[any]
   * and value=array[any]
   */
  private def keyBy(colNames: Seq[String], valueRdd: RDD[Array[Any]]) = {
    val columns = colNames.map {
      cols(_).index
    }
    ColumnZipper(this, columns)
  }.zip(valueRdd)

  /**
   * aggregate one column after grouping by another
   * @param aggByCol the columns to group by
   * @param aggedCol the columns to be aggregated, only one for now TBD
   * @param aggtor implementation of Aggregator
   * @tparam U
   * @tparam V
   * @return new DF with first column aggByCol and second aggedCol
   */
  def aggregate[U: ClassTag, V: ClassTag, W: ClassTag]
  (aggByCol: String, aggedCol: String, aggtor: Aggregator[U, V, W]) = {
    aggregateWithColumnStrategy(List(aggByCol), aggedCol, aggtor)
  }

  /**
   * aggregate one column after grouping by another
   */
  private def aggregateWithColumnStrategy[U: ClassTag, V: ClassTag, W: ClassTag]
  (aggdByCols: Seq[String], aggdCol: String, aggtor: Aggregator[U, V, W]) = {

    val wtpe = classTag[W]
    require(cols(aggdCol).compareType(classTag[U]))

    val newDf = DF(sc, s"${name}/${aggdCol}_aggby_${aggdByCols.mkString(";")}")

    val aggedRdd = keyBy(aggdByCols, aggdCol).asInstanceOf[RDD[(List[Any], U)]]
      .combineByKey(aggtor.convert, aggtor.mergeValue, aggtor.mergeCombiners)

    // columns of key
    var i = 0
    aggdByCols.foreach { aggdByCol =>
      val j = i
      if (cols(aggdByCol).isDouble) {
        val col1 = aggedRdd.map { case (k, v) =>
          k(j).asInstanceOf[Double]
        }
        newDf.update(aggdByCol, Column(sc, col1, newDf.numCols))
      } else if (cols(aggdByCol).isString) {
        val col1 = aggedRdd.map { case (k, v) =>
          k(j).asInstanceOf[String]
        }
        newDf.update(aggdByCol, Column(sc, col1, newDf.numCols))
      } else {
        println("ERROR: aggregate key type" + cols(aggdByCol).getType)
      }
      i += 1
    }

    // finalize the aggregations and add column of that
    if (wtpe == classTag[Double]) {
      val col1 = aggedRdd.map { case (k, v) =>
        aggtor.finalize(v)
      }.asInstanceOf[RDD[Double]]
      newDf.update(aggdCol, Column(sc, col1, 1))
    } else if (wtpe == classTag[String]) {
      val col1 = aggedRdd.map { case (k, v) =>
        aggtor.finalize(v)
      }.asInstanceOf[RDD[String]]
      newDf.update(aggdCol, Column(sc, col1, newDf.numCols))
    } else {
      println("ERROR: aggregate value type" + wtpe)
    }

    newDf
  }

  private def keyBy(colNames: Seq[String], valueCol: String) = {
    val columns = colNames.map {
      cols(_).index
    }
    ColumnZipper.zip(this, columns)
  }.zip(cols(valueCol).rdd)

  /**
   * aggregate multiple columns after grouping by multiple other columns
   * @param aggByCols sequence of columns to group by
   * @param aggedCol sequence of columns to be aggregated
   * @param aggtor implementation of Aggregator
   * @tparam U
   * @tparam V
   * @return new DF with first column aggByCol and second aggedCol
   */
  def aggregate[U: ClassTag, V: ClassTag, W: ClassTag]
  (aggByCols: Seq[String], aggedCol: Seq[String], aggtor: Aggregator[U, V, W]) = {
    aggregateWithColumnStrategy(aggByCols, aggedCol.head, aggtor)
  }

  /**
   * pivot the df and return a new df
   * e.g. half-yearly sales in <salesperson, period H1 or H2, sales> format
   * Jack, H1, 20
   * Jack, H2, 21
   * Jill, H1, 30
   * Gary, H2, 44
   * becomes "pivoted" to <salesperson, H1 sales, H2 sales>
   * Jack, 20, 21
   * Jill, 30, NaN
   * Gary, NaN, 44
   *
   * The resulting df will typically have fewer rows and more columns
   *
   * @param  keyCol column that has "primary key" for the pivoted df e.g. salesperson
   * @param pivotByCol column that is being removed e.g. period
   * @param pivotedCols columns that are being pivoted e.g. sales, by default all columns are pivoted
   * @return new pivoted DF
   */
  def pivot(keyCol: String, pivotByCol: String,
            pivotedCols: List[Int] = cols.values.map {
              _.index
            }.toList): DF = {
    val grped = groupBy(keyCol)
    val pivotValues = if (column(pivotByCol).isString)
      column(pivotByCol).distinct.collect.asInstanceOf[Array[String]]
    else if (column(pivotByCol).isDouble)
      column(pivotByCol).distinct.collect.asInstanceOf[Array[Double]].map {
        _.toString
      }
    else
      null
    val pivotIndex = cols.getOrElse(pivotByCol, null).index
    val cleanedPivotedCols = pivotedCols.filter {
      _ != cols(pivotByCol).index
    }

    val newDf = DF(sc, s"${name}_${keyCol}_pivot_${pivotByCol}")
    pivotValues.foreach { pivotValue =>
      val grpSplit = new PivotHelper(grped, pivotIndex, pivotValue).get
      cleanedPivotedCols.foreach { pivotedColIndex =>

        if (cols(colIndexToName(pivotedColIndex)).isDouble) {
          val newColRdd = grpSplit.map {
            case (k, v) =>
              if (v.isEmpty) Double.NaN else v.head(pivotedColIndex)
          }
          newDf.update(s"D_${colIndexToName(pivotedColIndex)}@$pivotByCol==$pivotValue",
            Column(sc, newColRdd.asInstanceOf[RDD[Double]]))
        } else if (cols(colIndexToName(pivotedColIndex)).isString) {
          val newColRdd = grpSplit.map {
            case (k, v) =>
              if (v.isEmpty) "" else v.head(pivotedColIndex)
          }
          newDf.update(s"S_${colIndexToName(pivotedColIndex)}@$pivotByCol==$pivotValue",
            Column(sc, newColRdd.asInstanceOf[RDD[String]]))
        } else
          println("Trouble, trouble! Unknown type in pivot")
      }
    }
    newDf
  }

  /**
   * get a column identified by name
   * @param colName name of the column
   */
  def column(colName: String) = {
    val col = cols.getOrElse(colName, null)
    if (null == col) println(s"${colName} not found")
    col
  }

  /**
   * update a column, add or replace
   */
  def update(colName: String, that: Column[Any]) = {
    val col = cols.getOrElse(colName, null)

    cols.put(colName, that)
    if (null != col) {
      println(s"Replaced Column: ${colName}")
      that.index = col.index
    } else {
      println(s"New Column: ${colName}")
      val colIndex = colIndexToName.size
      colIndexToName.put(colIndex, colName)
      that.index = colIndex
    }
    rowsRddCached = null //invalidate cached rows
    //FIXME: incremental computation of rows
  }

  /**
   * group by a column, uses a lot of memory. try to use aggregate(By) instead if possible
   */
  def groupBy(colName: String) = {
    keyBy(colName, rowsRdd).groupByKey
  }

  private def keyBy(colName: String, valueRdd: RDD[Array[Any]]) = {
    cols(colName).rdd
  }.zip(valueRdd)

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

  /**
   * print upto 10 x 10 elements of the dataframe
   */
  def list {
    println(s"Dimensions: $numRows x $numCols")
    val someRows = if (numRows <= 10) rowsRdd.collect else rowsRdd.take(10)
    def printRow(row: Array[Any]) {
      val someCols = if (row.length <= 10) row else row.take(10)
      println(someCols.mkString("\t"))
    }
    printRow((0 until numCols).map {
      colIndexToName(_)
    }.toArray)
    someRows.foreach {
      printRow _
    }
  }

  /*
   * add column keys, returns number of columns added
   */
  private def addHeader(header: Array[String]): Int = {
    addHeader(header.iterator)
  }

  /*
   * add column keys, returns number of columns added
   */
  private def addHeader(header: Iterator[String]) = {
    var i = 0
    header.foreach { colName =>
      val cleanedColName = if (cols.contains(colName)) {
        println(s"**WARN**: Duplicate column ${colName} renamed")
        colName + "_"
      } else {
        colName
      }
      cols.put(cleanedColName, null)
      colIndexToName.put(i, cleanedColName)
      i += 1
    }
    i
  }

}

object DF {

  /**
   * create DF from a text file with given separator
   * first line of file is a header
   */
  def apply(sc: SparkContext, inFile: String, separator: Char, fasterGuess: Boolean): DF = {
    val df: DF = DF(sc, "inFile")
    df.defaultStorageLevel = MEMORY_ONLY_SER //FIXME: allow changing this
    val csvFormat = CSVFormat.DEFAULT.withDelimiter(separator).withIgnoreSurroundingSpaces(true)
    val file = sc.textFile(inFile)
    val firstLine = file.first
    val header = CSVParser.parse(firstLine, csvFormat).iterator.next
    println(s"Found ${header.size} columns")
    df.addHeader(JavaConversions.asScalaIterator(header.iterator))

    val dataLines = file.mapPartitionsWithIndex({
      case (partitionIndex, iter) => if (partitionIndex == 0) iter.drop(1) else iter
    }, true)
    dataLines.setName(s"data/$inFile")

    /*
     * guess the type of a column by looking at a random sample
     * however, this is slow because spark will cause all data to be
     * materialized before it can be sampled
     */
    def guessType2(col: RDD[String]) = {
      val samples = col.sample(false, 0.01, (new Random).nextLong)
        .union(sc.parallelize(List(col.first)))
      val parseFailCount = samples.filter { str =>
        Try {
          str.toDouble
        }.toOption == None
      }.count

      if (parseFailCount > 0)
        ru.typeOf[String]
      else
        ru.typeOf[Double]
    }
    /*
     * guess the type of a column by looking at a random sample
     * however, this is slow because spark will cause all data to be
     * materialized before it can be sampled
     */
    def guessType3(col: RDD[String]) = {
      val samples = col.take(5)
      val parseFailCount = samples.filter { str =>
        Try {
          str.toDouble
        }.toOption == None
      }.length

      if (parseFailCount > 0)
        ru.typeOf[String]
      else
        ru.typeOf[Double]
    }

    /*
     * guess the type of a column by looking at the first element
     * in every partition. faster than random sampling method
     */
    def guessType(col: RDD[String]) = {
      val parseErrors = sc.accumulator(0)
      col.foreachPartition { str =>
        try {
          val y = if (str.hasNext) str.next.toDouble else 0
        } catch {
          case _: java.lang.NumberFormatException => parseErrors += 1
        }
      }
      if (parseErrors.value > 0)
        ru.typeOf[String]
      else
        ru.typeOf[Double]
    }

    val rows = dataLines.map {
      CSVParser.parse(_, csvFormat).iterator.next
    }
      .persist(df.defaultStorageLevel).setName(s"csvRecord/$inFile")
    val columns = for (i <- 0 until df.numCols) yield {
      rows.map {
        _.get(i)
      }.persist(df.defaultStorageLevel)
    }

    var i = 0
    columns.foreach { col =>
      val t = if (fasterGuess) guessType3(columns(i)) else guessType(columns(i))
      col.setName(s"$t/$inFile/${df.colIndexToName(i)}")
      println(s"Column: ${df.colIndexToName(i)} \t\t\tGuessed Type: ${t}")
      if (t == ru.typeOf[Double]) {
        df.cols.put(df.colIndexToName(i), Column.asDoubles(sc, col, i, df.defaultStorageLevel))
        col.unpersist()
      } else {
        df.cols.put(df.colIndexToName(i), Column(sc, col, i))
        col.persist(df.defaultStorageLevel)
      }
      i += 1
    }
    //rows.unpersist()
    df
  }

  /**
   * create a DF given column names and vectors of columns(not rows)
   */
  def apply(sc: SparkContext, header: Vector[String], vec: Vector[Vector[Any]]): DF = {
    require(header.length == vec.length)
    require(vec.map {
      _.length
    }.toSet.size == 1)

    val df = DF(sc, "from_vector")
    df.addHeader(header.toArray)

    var i = 0
    vec.foreach { col =>
      col(0) match {
        case c: Double =>
          println(s"Column: ${df.colIndexToName(i)} Type: Double")
          df.cols.put(df.colIndexToName(i),
            Column(sc, sc.parallelize(col.asInstanceOf[Vector[Double]]), i))

        case c: String =>
          println(s"Column: ${df.colIndexToName(i)} Type: String")
          df.cols.put(df.colIndexToName(i),
            Column(sc, sc.parallelize(col.asInstanceOf[Vector[String]]), i))
      }
      i += 1
    }
    df
  }

  /**
   * create a DF from a ColumnSeq
   */
  def apply(sc: SparkContext, colSeq: ColumnSeq): DF = {
    val df = DF(sc, "colseq")
    val header = colSeq.cols.map {
      _._1
    }
    val columns = colSeq.cols.map {
      _._2
    }

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
  def apply(sc: SparkContext, name: String, col: Column[Double]): DF = {
    val df = DF(sc, "col")
    val i = df.addHeader(Array(name))
    df.cols.put(df.colIndexToName(i - 1), col)
    df
  }

  /**
   * make a filtered DF
   */
  def apply(df: DF, filtration: RDD[Boolean]) = {
    val cols = df.cols.clone
    for (i <- 0 until df.numCols) {
      def applyFilter[T: ClassTag](in: RDD[T]) = {
        in.zip(filtration).filter {
          _._2 == true
        }.map {
          _._1
        }
      }
      val colName = df.colIndexToName(i)
      val col = cols(colName)

      if (col.isDouble)
        cols(colName) = Column(df.sc, applyFilter(col.doubleRdd), i)
      else if (col.isString)
        cols(colName) = Column(df.sc, applyFilter(col.stringRdd), i)
      else
        println("Unexpected column type while column strategy filtering")
    }
    println(cols)
    new DF(df.sc, cols, df.colIndexToName.clone, "filtered")
  }

  /**
   * relational-like join two DFs
   */
  def join(sc: SparkContext, left: DF, right: DF, on: String, how: JoinType.JoinType = JoinType.Inner) = {
    val df = DF(sc, "joined")
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
        if (t == ru.typeOf[Double]) {
          val colRdd = joinedRows.map { row => partGetter(row)(origIndex).asInstanceOf[Double]}
          val column = Column(curDf.sc, colRdd, joinedIndex)
          df.cols.put(newColName, column)
        } else if (t == ru.typeOf[String]) {
          val colRdd = joinedRows.map { row => partGetter(row)(origIndex).asInstanceOf[String]}
          val column = Column(curDf.sc, colRdd, joinedIndex)
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

  /**
   * make an empty DF
   */
  def apply(sc: SparkContext, name: String) = {
    new DF(sc, new HashMap[String, Column[Any]], new HashMap[Int, String], name)
  }

  def joinRdd(sc: SparkContext, left: DF, right: DF, on: String, how: JoinType.JoinType = JoinType.Inner) = {
    val leftWithKey = left.cols(on).rdd.zip(left.rowsRdd)
    val rightWithKey = right.cols(on).rdd.zip(right.rowsRdd)
    leftWithKey.join(rightWithKey)
  }

  def getType(elem: Any) = {
    elem match {
      case x: Double => ru.typeOf[Double]
      case x: String => ru.typeOf[String]
      case _ => {
        println(s"PANIC: unsupported column type ${elem}")
        null
      }
    }
  }
}
