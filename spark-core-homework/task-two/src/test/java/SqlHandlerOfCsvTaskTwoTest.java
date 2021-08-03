import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class SqlHandlerOfCsvTaskTwoTest {

    @Test
    public void prepareSqlQueryTextTestShouldReturnCorrectSqlQueryText() {
        String correctSqlQueryText =
                "SELECT hotel_country,\n" +
                        "COUNT(*) AS number_of_rows\n" +
                        "FROM train\n" +
                        "WHERE hotel_country = user_location_country\n" +
                        "GROUP BY hotel_country\n" +
                        "ORDER BY number_of_rows DESC\n" +
                        "LIMIT 10";
        Assert.assertEquals(correctSqlQueryText, SqlHandlerOfCsvTaskTwo.prepareSqlQueryText());
    }

}