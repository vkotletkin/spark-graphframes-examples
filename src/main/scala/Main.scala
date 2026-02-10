import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties
import scala.collection.parallel.CollectionConverters._
import scala.util.{Success, Try}

case class SourceConfig(
                         id: Int,
                         sourceName: String,
                         query: String,
                         address: String,
                         username: String,
                         password: String,
                         numPartitions: String,
                         active: Boolean
                       )

object Main extends LazyLogging {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark 4.1.0 Test")
      .master("local[*]")
      .getOrCreate()

    try {

      logger.info("Starting load configs for data sources")
      val sourcesConfigs = loadSourceConfigs(spark)

      logger.info("Starting execute commands for data sources")
      val dataframes = executeCommands(spark, sourcesConfigs)

      logger.info("Starting combine dataframes")
      val combinedDF = combineDataFrames(spark, dataframes)

      combinedDF.show()
    }
    finally {
      spark.stop()
    }
  }


  private def loadSourceConfigs(spark: SparkSession): List[SourceConfig] = {

    import spark.implicits._

    spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/postgres")
      .option("dbtable",
        """(
          |SELECT
          |id,
          |source_name as sourceName,
          |query,
          |address,
          |username,
          |password,
          |num_partitions as numPartitions,
          |active
          |FROM public.ingestion_registry
          |WHERE active = true
          |) as temp_table""".stripMargin)
      .option("user", "postgres")
      .option("password", "postgres")
      .load()
      .as[SourceConfig]
      .collect()
      .toList
  }

  private def executeCommands(spark: SparkSession, configs: List[SourceConfig]): List[DataFrame] = {

    logger.info(s"Start reading ${configs.size} data sources...")

    // Используем параллельные коллекции для одновременного выполнения
    val results = configs.par.map { config =>
      (config, executeCommandWithRetry(spark, config))
    }.toList

    // Фильтруем успешные результаты и добавляем source_id
    results.collect {
      case (config, Success(df)) =>
        df.withColumn("source_id", lit(config.id))
    }
  }

  private def executeCommandWithRetry(spark: SparkSession, config: SourceConfig, maxRetries: Int = 3): Try[DataFrame] = {

    def attempt(attemptNumber: Int): Try[DataFrame] = Try {
      executeCommand(spark, config, attemptNumber)
    }.recoverWith {
      case e: Exception if attemptNumber <= maxRetries =>
        logger.warn(s"Retry ${attemptNumber + 1}/$maxRetries for source ${config.id}: ${e.getMessage}")
        Thread.sleep(1000 * (attemptNumber + 1))
        attempt(attemptNumber + 1)
    }

    attempt(1)
  }

  private def executeCommand(spark: SparkSession, config: SourceConfig, attempt: Int = 0): DataFrame = {

    val props = new Properties()
    props.put("user", config.username)
    props.put("password", config.password)

    //    // Определяем драйвер по URL
    //    val driver = if (config.jdbcUrl.contains("clickhouse")) {
    //      props.put("socket_timeout", "300000")
    //      "ru.yandex.clickhouse.ClickHouseDriver"
    //    } else if (config.jdbcUrl.contains("postgresql")) {
    //      "org.postgresql.Driver"
    //    } else if (config.jdbcUrl.contains("mysql")) {
    //      "com.mysql.cj.jdbc.Driver"
    //    } else if (config.jdbcUrl.contains("oracle")) {
    //      "oracle.jdbc.OracleDriver"
    //    } else if (config.jdbcUrl.contains("sqlserver")) {
    //      "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    //    } else {
    //      throw new IllegalArgumentException(s"Неизвестный драйвер для URL: ${config.jdbcUrl}")
    //    }

    val driver = "org.postgresql.Driver"
    props.put("driver", driver)

    props.put("fetchSize", "10000")
    props.put("socketTimeout", "300000")

    //    // Формируем SQL вызов хранимой процедуры
    //    val sqlQuery = buildProcedureCall(config.procedureName, config.params)

    spark.read.jdbc(
      url = s"jdbc:postgresql://${config.address}/postgres",
      table = s"(${config.query}) as procedure_result",
      properties = props
    )
  }

  private def buildProcedureCall(procedureName: String, params: Map[String, String]): String = {
    if (params.isEmpty) {
      s"EXEC $procedureName"
    } else {
      val paramString = params.map { case (k, v) => s"$k='$v'" }.mkString(", ")
      s"EXEC $procedureName $paramString"
    }
  }

  private def combineDataFrames(spark: SparkSession, dataframes: List[DataFrame]): DataFrame = {
    if (dataframes.isEmpty) {
      import spark.implicits._
      // Возвращаем DataFrame с базовой схемой
      Seq.empty[(Int, String, String)].toDF("source_id", "source_name", "error_message")
    } else if (dataframes.size == 1) {
      dataframes.head
    } else {
      // Пакетное объединение для лучшей производительности
      val batchSize = 20
      dataframes
        .grouped(batchSize)
        .map { batch =>
          batch.reduce((df1, df2) => df1.unionByName(df2, allowMissingColumns = true))
        }
        .reduce((df1, df2) => df1.unionByName(df2, allowMissingColumns = true))
    }
  }
}