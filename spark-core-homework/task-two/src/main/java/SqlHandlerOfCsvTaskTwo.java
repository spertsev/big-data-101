public class SqlHandlerOfCsvTaskTwo {

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
