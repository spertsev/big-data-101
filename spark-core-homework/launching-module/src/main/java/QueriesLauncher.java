/**
 * This class runs querying of the train.csv file.
 */
public class QueriesLauncher {

    /**
     * This method runs querying of the train.csv file. For example, syntax of running on a Spark-cluster (the project
     * should be built into a jar with dependencies):
     * "spark-submit --class QueriesLauncher launching-project-1.0-SNAPSHOT-jar-with-dependencies.jar
     * hdfs://sandbox-hdp.hortonworks.com:8020/user/admin/expedia-dataset-renamed/train.csv"
     *
     * @param args are JVM parameters. args[0] is used, it is a path of the csv file, which is processed (can be a file
     *             system, HDFS, others supported by Spark).
     * @throws InterruptedException if something goes wrong.
     */
    public static void main(String[] args) throws InterruptedException {
        String trainCsvUri = args[0];
        SqlHandlerBase.makeDatasetWithQueryingCsv(trainCsvUri, SqlHandlerOfCsvTaskOne.prepareSqlQueryText()).show();
        Thread.sleep(10000);
        SqlHandlerBase.makeDatasetWithQueryingCsv(trainCsvUri, SqlHandlerOfCsvTaskTwo.prepareSqlQueryText()).show();
        Thread.sleep(10000);
        SqlHandlerBase.makeDatasetWithQueryingCsv(trainCsvUri, SqlHandlerOfCsvTaskThree.prepareSqlQueryText()).show();
    }

}
