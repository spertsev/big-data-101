public class SqlHandlerOfCsvTaskOne {

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
