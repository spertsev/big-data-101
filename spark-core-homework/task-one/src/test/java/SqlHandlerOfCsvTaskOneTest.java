import org.junit.Assert;
import org.junit.Test;

public class SqlHandlerOfCsvTaskOneTest {

    @Test
    public void prepareSqlQueryTextTestShouldReturnCorrectSqlQueryText() {
        String correctSqlQueryText =
                "SELECT hotel_continent,\n" +
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
        Assert.assertEquals(correctSqlQueryText, SqlHandlerOfCsvTaskOne.prepareSqlQueryText());
    }

}