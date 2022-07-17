package vlp.tok

import org.apache.spark.sql.SparkSession
import scopt.OptionParser
import java.io.File
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.SaveMode

case class ConfigTokenizer(
    master: String = "local[*]",
    driverHost: String = "0.0.0.0",
    uiPort: Int = 9696,
    totalCores: Int = 9,
    executorCores: Int = 3,
    executorMemory: String = "8g",
    driverMemory: String = "8g",
    minPartitions: Int = 1,
    inputPath: String = "",
    inputFormat: String = "json", // json/text/vne
    inputColumnName: String = "content",
    outputColumnName: String = "text",
    outputPath: String = ""
)

/** phuonglh@gmail.com
  */
object VietnameseTokenizer {

  def readText(
      sparkSession: SparkSession,
      config: ConfigTokenizer
  ): DataFrame = {
    val rdd =
      if (new File(config.inputPath).isFile())
        sparkSession.sparkContext.textFile(
          config.inputPath,
          config.minPartitions
        )
      else
        sparkSession.sparkContext
          .wholeTextFiles(config.inputPath, config.minPartitions)
          .map(_._2)

    val rows = rdd.flatMap(content => content.split("""\n+""")).map(Row(_))
    val schema = new StructType().add(StructField("content", StringType, true))
    sparkSession.createDataFrame(rows, schema)
  }

  def readVNE(
      sparkSession: SparkSession,
      config: ConfigTokenizer
  ): DataFrame = {
    val rdd =
      sparkSession.sparkContext.textFile(config.inputPath, config.minPartitions)
    val rows = rdd.map(line => {
      val j = line.indexOf('\t')
      val category = line.substring(0, j).trim
      val content = line.substring(j + 1).trim
      Row(category, content)
    })
    val schema = new StructType()
      .add(StructField("category", StringType, true))
      .add(StructField("content", StringType, true))
    sparkSession.createDataFrame(rows, schema)
  }

  /** Reads a line-oriented JSON file. Number of lines is the number of rows of
    * the resulting data frame.
    *
    * @param sparkSession
    * @param config
    * @return
    *   a data frame.
    */
  def readJson(
      sparkSession: SparkSession,
      config: ConfigTokenizer
  ): DataFrame = {
    sparkSession.read.json(config.inputPath)
  }

  def readJsonMultiline(sparkSession: SparkSession, path: String): DataFrame = {
    sparkSession.read
      .option("multiLine", "true")
      .option("mode", "PERMISSIVE")
      .json(path)
  }

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser[ConfigTokenizer]("vlp.tok.Tokenizer") {
      head("vlp.tok.VietnamseTokenizer", "1.0")
      opt[String]('m', "master")
        .action((x, conf) => conf.copy(master = x))
        .text("Spark master, default is local[*]")
      opt[String]('h', "driverHost")
        .action((x, conf) => conf.copy(driverHost = x))
        .text("driver host, default is 0.0.0.0")
      opt[Int]('r', "uiPort")
        .action((x, conf) => conf.copy(uiPort = x))
        .text("ui port, default is 9696")
      opt[Int]('t', "totalCores")
        .action((x, conf) => conf.copy(totalCores = x))
        .text("total number of cores, default is 9")
      opt[Int]('c', "executorCores")
        .action((x, conf) => conf.copy(executorCores = x))
        .text("executor cores, default is 3")
      opt[String]('e', "executorMemory")
        .action((x, conf) => conf.copy(executorMemory = x))
        .text("executor memory, default is 8g")
      opt[String]('d', "driverMemory")
        .action((x, conf) => conf.copy(driverMemory = x))
        .text("driver memory, default is 8g")
      opt[Int]('p', "partitions")
        .action((x, conf) => conf.copy(minPartitions = x))
        .text("min partitions")
      opt[String]('i', "inputPath")
        .action((x, conf) => conf.copy(inputPath = x))
        .text("input path (a text file or a directory of .txt/.json files)")
      opt[String]('f', "inputFormat")
        .action((x, conf) => conf.copy(inputFormat = x))
        .text("input format, default is 'json'")
      opt[String]('u', "inputColumnName")
        .action((x, conf) => conf.copy(inputColumnName = x))
        .text("input column name, default is 'content'")
      opt[String]('v', "outputColumnName")
        .action((x, conf) => conf.copy(outputColumnName = x))
        .text("output column name, default is 'text'")
      opt[String]('o', "outputPath")
        .action((x, conf) => conf.copy(outputPath = x))
        .text("output path which is a directory containing output JSON files")
    }
    parser.parse(args, ConfigTokenizer()) match {
      case Some(config) =>
        val sparkSession = SparkSession
          .builder()
          .appName("vietnameseTokenizer")
          .master(config.master)
          .config("spark.cores.max", config.totalCores.toString)
          .config("spark.executor.cores", config.executorCores.toString)
          .config("spark.executor.memory", config.executorMemory)
          .config("spark.driver.memory", config.driverMemory)
          .config("spark.driver.host", config.driverHost)
          .config("spark.ui.port", config.uiPort.toString)
          .config("spark.rpc.message.maxSize", "1024")
          .getOrCreate()

        val df = config.inputFormat match {
          case "json"      => readJson(sparkSession, config)
          case "text"      => readText(sparkSession, config)
          case "vnexpress" => readVNE(sparkSession, config)
          case "shinra"    => readJsonMultiline(sparkSession, config.inputPath)
        }
        df.printSchema()

        val tokenizer = new TokenizerTransformer()
          .setSplitSentences(true)
          .setInputCol(config.inputColumnName)
          .setOutputCol(config.outputColumnName)
        val tokenized = tokenizer.transform(df)
        val result = config.inputFormat match {
          case "vnexpress" => tokenized.select("category", "text")
          case "shinra" =>
            tokenized.select(
              "id",
              config.outputColumnName,
              "title",
              "category",
              "outgoingLink",
              "redirect",
              "clazz"
            )
          case _ => tokenized.select("text")
        }
        println(s"Number of texts = ${result.count()}")
        result
          .repartition(config.minPartitions)
          .write
          .mode(SaveMode.Overwrite)
          .json(config.outputPath)
        sparkSession.stop()
      case None =>
    }
  }
}
