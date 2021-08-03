/**
 * This class is a part of the multi-modules project for querying the train.csv with Spark SQL. SqlHandlerOfCsvTaskTwo
 * contains the method used in the launching module as the second part for processing the csv file (methods of
 * the base-module is the first part)
 */
public class SqlHandlerOfCsvTaskTwo {

    /**
     * This method prepares an sql query for processing the csv.
     *
     * @return text of an sql query for processing the csv.
     */
    public static String prepareSqlQueryText() {
        return "SELECT hotel_country,\n" +
                "COUNT(*) AS number_of_rows\n" +
                "FROM train\n" +
                "WHERE hotel_country = user_location_country\n" +
                "GROUP BY hotel_country\n" +
                "ORDER BY number_of_rows DESC\n" +
                "LIMIT 10";
    }

}
