import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
//
public class SqlHandlerOfCsvTaskOne {

    public static void processCsvWithFirstQuery() throws InterruptedException {
        String csvPath = "hdfs://sandbox-hdp.hortonworks.com:8020/user/admin/expedia-dataset-renamed/train.csv";
        String sparkAppName = "Java Spark SQL queries for train.csv";

        SparkSession sparkSession = SparkSession
                .builder()
                .appName(sparkAppName)
                .getOrCreate();

        StructType trainSchema = new StructType()
                .add("date_time", "string")
                .add("site_name", "int")
                .add("posa_continent", "int")
                .add("user_location_country", "int")
                .add("user_location_region", "int")
                .add("user_location_city", "int")
                .add("orig_destination_distance", "double")
                .add("user_id", "int")
                .add("is_mobile", "tinyint")
                .add("is_package", "int")
                .add("channel", "int")
                .add("srch_ci", "string")
                .add("srch_co", "string")
                .add("srch_adults_cnt", "int")
                .add("srch_children_cnt", "int")
                .add("srch_rm_cnt", "int")
                .add("srch_destination_id", "int")
                .add("srch_destination_type_id", "int")
                .add("is_booking", "tinyint")
                .add("cnt", "bigint")
                .add("hotel_continent", "int")
                .add("hotel_country", "int")
                .add("hotel_market", "int")
                .add("hotel_cluster", "int");

        Dataset<Row> trainDataset = sparkSession
                .read()
                .option("mode", "DROPMALFORMED")
                .schema(trainSchema)
                .csv(csvPath);

        trainDataset.createOrReplaceTempView("train");
        Dataset<Row> trainSqlDataset = sparkSession.sql(
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

        trainSqlDataset.show();
        Thread.sleep(1 * 60 * 1000);
    }
}
