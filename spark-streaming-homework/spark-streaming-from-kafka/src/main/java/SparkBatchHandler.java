import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkBatchHandler {
    public static final String APP_NAME = "Spark Batch Handler";
    public static final String RESOURCE_MANAGER = "local[1]";

    public static void main(String[] args) {
        String sourceCsvPath = "C:\\Users\\SuperUser\\Downloads\\expedia-hotel-recommendations\\train-short.csv";
        String targetFilePath = "C:\\Users\\SuperUser\\Downloads\\expedia-hotel-recommendations\\out\\target.txt";

        SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster(RESOURCE_MANAGER);
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(sourceCsvPath);
        lines.saveAsTextFile(targetFilePath);
    }
}
