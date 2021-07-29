public class QueriesLauncher {

    public static void main(String[] args) throws InterruptedException {
        String trainCsvUri = args[0];

        SqlHandlerOfCsvTaskOne.processCsvWithFirstQuery(trainCsvUri);
        SqlHandlerOfCsvTaskTwo.processCsvWithSecondQuery(trainCsvUri);
        SqlHandlerOfCsvTaskThree.processCsvWithThirdQuery(trainCsvUri);

    }
}
