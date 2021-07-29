import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class SqlHandlerOfCsvTaskTwo extends SqlHandlerBase {

    public static void processCsvWithSecondQuery(String trainCsvPath) throws InterruptedException {
        StructType trainCsvSchema = prepareTrainCsvSchema();
        SparkSession sparkSession = createSparkSession();
        Dataset<Row> trainCsvDataset = makeDatasetByReadingCsv(trainCsvSchema, sparkSession, trainCsvPath);

        trainCsvDataset.createOrReplaceTempView("train");
        Dataset<Row> trainCsvSqlDataset = sparkSession.sql(
                "SELECT hotel_country,\n" +
                        "COUNT(*) AS number_of_rows\n" +
                        "FROM train\n" +
                        "WHERE hotel_country = user_location_country\n" +
                        "GROUP BY hotel_country\n" +
                        "ORDER BY number_of_rows DESC\n" +
                        "LIMIT 10");

        trainCsvSqlDataset.show();
        Thread.sleep(1 * 60 * 1000);
    }
}
