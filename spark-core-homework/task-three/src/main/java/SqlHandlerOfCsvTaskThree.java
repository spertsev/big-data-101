public class SqlHandlerOfCsvTaskThree {

    public static String prepareSqlQueryText() {
        return "SELECT hotel_continent,\n" +
                "       hotel_country,\n" +
                "       hotel_market,\n" +
                "       COUNT(*) AS number_of_rows\n" +
                "FROM train\n" +
                "WHERE (srch_children_cnt > 0) AND (is_booking = 0)\n" +
                "GROUP BY hotel_continent,\n" +
                "         hotel_country,\n" +
                "         hotel_market\n" +
                "ORDER BY number_of_rows DESC\n" +
                "LIMIT 3";
    }

}
