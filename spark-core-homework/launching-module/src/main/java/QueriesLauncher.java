public class QueriesLauncher {

    public static void main(String[] args) throws InterruptedException {

        SqlHandlerOfCsvTaskOne.processCsvWithFirstQuery();
        SqlHandlerOfCsvTaskTwo.processCsvWithSecondQuery();
        SqlHandlerOfCsvTaskThree.processCsvWithThirdQuery();

    }
}
