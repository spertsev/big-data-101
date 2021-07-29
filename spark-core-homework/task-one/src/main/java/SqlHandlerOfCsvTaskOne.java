import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class SqlHandlerOfCsvTaskOne extends SqlHandlerBase {

    public static void processCsvWithFirstQuery(String trainCsvPath) throws InterruptedException {
        StructType trainCsvSchema = prepareTrainCsvSchema();
        SparkSession sparkSession = createSparkSession();
        Dataset<Row> trainCsvDataset = makeDatasetByReadingCsv(trainCsvSchema, sparkSession, trainCsvPath);

        trainCsvDataset.createOrReplaceTempView("train");
        Dataset<Row> trainCsvSqlDataset = sparkSession.sql(
                "SELECT hotel_continent,\n" +
                        "       hotel_country,\n" +
                        "       hotel_market,\n" +
                        "       COUNT(*) AS number_of_rows\n" +
                        "FROM train\n" +
                        "WHERE srch_adults_cnt = 2\n" +
                        "GROUP BY hotel_continent,\n" +
                        "         hotel_country,\n" +
                        "         hotel_market\n" +
                        "ORDER BY number_of_rows DESC\n" +
                        "LIMIT 3");

        trainCsvSqlDataset.show();
        Thread.sleep(1 * 60 * 1000);
    }
}
