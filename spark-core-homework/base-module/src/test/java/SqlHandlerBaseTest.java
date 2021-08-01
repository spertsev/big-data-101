import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class SqlHandlerBaseTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void prepareTrainCsvSchemaShouldReturnCorrectCsvSchemaTest() {
        StructType csvSchema = SqlHandlerBase.prepareTrainCsvSchema();
        String controlStringRepresentationOfCsvScheme = "STRUCT<`date_time`: STRING, `site_name`: INT, `posa_continent`: INT, `user_location_country`: INT, `user_location_region`: INT, `user_location_city`: INT, `orig_destination_distance`: DOUBLE, `user_id`: INT, `is_mobile`: TINYINT, `is_package`: INT, `channel`: INT, `srch_ci`: STRING, `srch_co`: STRING, `srch_adults_cnt`: INT, `srch_children_cnt`: INT, `srch_rm_cnt`: INT, `srch_destination_id`: INT, `srch_destination_type_id`: INT, `is_booking`: TINYINT, `cnt`: BIGINT, `hotel_continent`: INT, `hotel_country`: INT, `hotel_market`: INT, `hotel_cluster`: INT>";
        Assert.assertEquals(csvSchema.sql(), controlStringRepresentationOfCsvScheme);
    }

    @Test
    public void createSparkSessionShouldReturnSparkSessionObjectTest() {
        Assert.assertEquals("org.apache.spark.sql.SparkSession", SqlHandlerBase.createSparkSession().getClass().getName());
    }

    @Test
    public void makeDatasetByReadingCsvShouldReturnCorrectDatasetTest() {
        File testCsv;
        Dataset<Row> datasetMadeByReadingCsv;
        StructType csvSchema = SqlHandlerBase.prepareTrainCsvSchema();
        SparkSession sparkSession = SqlHandlerBase.createSparkSession();
        try {
            testCsv = tempFolder.newFile("test.csv");
            FileWriter fileWriter = new FileWriter(testCsv);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
            String testCsvContent = "date_time,site_name,posa_continent,user_location_country,user_location_region,user_location_city,orig_destination_distance,user_id,is_mobile,is_package,channel,srch_ci,srch_co,srch_adults_cnt,srch_children_cnt,srch_rm_cnt,srch_destination_id,srch_destination_type_id,is_booking,cnt,hotel_continent,hotel_country,hotel_market,hotel_cluster\n" +
                    "8/11/2014 7:46,2,3,66,348,48862,2234.2641,12,0,1,9,8/27/2014,8/31/2014,2,0,1,8250,1,0,3,2,50,628,1\n" +
                    "8/11/2014 8:22,2,3,66,348,48862,2234.2641,12,0,1,9,8/29/2014,9/2/2014,2,1,1,8250,1,1,1,2,66,628,1\n" +
                    "8/11/2014 8:24,2,3,100,348,48862,2234.2641,12,0,0,9,8/29/2014,9/2/2014,2,0,1,8250,1,0,1,2,100,628,1\n" +
                    "8/9/2014 18:05,2,3,100,442,35390,913.1932,93,0,0,3,11/23/2014,11/28/2014,2,0,1,14984,1,0,1,2,100,1457,80\n" +
                    "8/9/2014 18:08,2,3,66,442,35390,913.6259,93,0,0,3,11/23/2014,11/28/2014,2,2,1,14984,1,0,1,2,50,1457,21\n" +
                    "8/9/2014 18:13,2,3,200,442,35390,911.5142,93,0,0,3,11/23/2014,11/28/2014,2,0,1,14984,1,0,1,2,200,1457,92\n" +
                    "7/16/2014 9:42,2,3,200,189,10067,,501,0,0,2,8/1/2014,8/2/2014,2,1,1,8267,1,0,2,2,200,675,41\n" +
                    "7/16/2014 9:45,2,3,200,189,10067,,501,0,1,2,8/1/2014,8/2/2014,2,1,1,8267,1,0,1,2,200,675,41\n" +
                    "7/16/2014 9:52,2,3,66,189,10067,,501,0,0,2,8/1/2014,8/2/2014,2,1,1,8267,1,0,1,2,50,675,69\n" +
                    "7/16/2014 9:55,2,3,66,189,10067,,501,0,0,2,8/1/2014,8/2/2014,2,1,1,8267,1,0,1,2,50,675,70";
            bufferedWriter.write(testCsvContent);
            bufferedWriter.close();
            datasetMadeByReadingCsv = SqlHandlerBase.makeDatasetByReadingCsv(csvSchema, sparkSession, testCsv.getAbsolutePath());
            Assert.assertEquals(11, datasetMadeByReadingCsv.count());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void processCsvWithSqlQueryTest() {

    }

}