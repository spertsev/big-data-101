import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Test;

public class SqlHandlerBaseTest {

    @Test
    public void prepareTrainCsvSchemaTestShouldReturnCorrectCsvSchema() {
        StructType csvSchema = SqlHandlerBase.prepareTrainCsvSchema();
        String controlStringRepresentationOfCsvScheme = "STRUCT<`date_time`: STRING, `site_name`: INT, `posa_continent`: INT, `user_location_country`: INT, `user_location_region`: INT, `user_location_city`: INT, `orig_destination_distance`: DOUBLE, `user_id`: INT, `is_mobile`: TINYINT, `is_package`: INT, `channel`: INT, `srch_ci`: STRING, `srch_co`: STRING, `srch_adults_cnt`: INT, `srch_children_cnt`: INT, `srch_rm_cnt`: INT, `srch_destination_id`: INT, `srch_destination_type_id`: INT, `is_booking`: TINYINT, `cnt`: BIGINT, `hotel_continent`: INT, `hotel_country`: INT, `hotel_market`: INT, `hotel_cluster`: INT>";
        Assert.assertEquals(csvSchema.sql(), controlStringRepresentationOfCsvScheme);
    }

    @Test
    public void createSparkSessionTestShouldReturnSparkSessionObject() {
        Assert.assertEquals("class org.apache.spark.sql.SparkSession", SqlHandlerBase.createSparkSession().getClass().toString());
    }

}