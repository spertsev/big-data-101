public class QueriesLauncher {

    public static void main(String[] args) throws InterruptedException {
        String trainCsvUri = args[0];
        SqlHandlerBase.makeDatasetWithQueryingCsv(trainCsvUri, SqlHandlerOfCsvTaskOne.prepareSqlQueryText()).show();
        Thread.sleep(10000);
        SqlHandlerBase.makeDatasetWithQueryingCsv(trainCsvUri, SqlHandlerOfCsvTaskTwo.prepareSqlQueryText()).show();
        Thread.sleep(10000);
        SqlHandlerBase.makeDatasetWithQueryingCsv(trainCsvUri, SqlHandlerOfCsvTaskThree.prepareSqlQueryText()).show();
    }

}
