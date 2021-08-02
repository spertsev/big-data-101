import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class SqlHandlerBase {

    public static StructType prepareTrainCsvSchema() {
        return new StructType()
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
    }

    public static Dataset<Row> makeDatasetWithQueryingCsv(String trainCsvPath, String sqlQueryText) {
        StructType trainCsvSchema = prepareTrainCsvSchema();
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Java Spark SQL queries for train.csv")
                .config("spark.master", "local[1]")
                .getOrCreate();
        Dataset<Row> trainCsvDataset = sparkSession
                .read()
                .option("mode", "DROPMALFORMED")
                .schema(trainCsvSchema)
                .csv(trainCsvPath);
        trainCsvDataset.createOrReplaceTempView("train");
        return sparkSession.sql(sqlQueryText);
    }

}
