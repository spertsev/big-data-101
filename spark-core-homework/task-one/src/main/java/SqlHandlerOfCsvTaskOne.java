/**
 * This class is a part of the multi-modules project for querying the train.csv with Spark SQL. SqlHandlerOfCsvTaskOne
 * contains the method used in the launching module as the second part for processing the csv file (methods of
 * the base-module is the first part)
 */
public class SqlHandlerOfCsvTaskOne {

    /**
     * This method prepares an sql query for processing the csv.
     *
     * @return text of an sql query for processing the csv.
     */
    public static String prepareSqlQueryText() {
        return "SELECT hotel_continent,\n" +
                "       hotel_country,\n" +
                "       hotel_market,\n" +
                "       COUNT(*) AS number_of_rows\n" +
                "FROM train\n" +
                "WHERE srch_adults_cnt = 2\n" +
                "GROUP BY hotel_continent,\n" +
                "         hotel_country,\n" +
                "         hotel_market\n" +
                "ORDER BY number_of_rows DESC\n" +
                "LIMIT 3";
    }

}
