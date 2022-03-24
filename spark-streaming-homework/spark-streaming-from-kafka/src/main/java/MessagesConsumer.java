import kafka.serializer.StringDecoder;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;
//import scala.Tuple2;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MessagesConsumer {

    public static String TOPIC = "booking_topic";
    //public static String HOST = "sandbox-hdp.hortonworks.com:6667";
    public static String HOST = "localhost:9092";

    public static void main(String[] args) {

        //String targetFolderPath = args[0];
        String targetFolderPath = "C:\\Users\\SuperUser\\Downloads\\output-folder\\rdd-folder\\hdfs-1";


        SparkConf conf = new SparkConf().setAppName("consumer-from-kafka").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(10000));
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", HOST);
//        Set<String> topics = Collections.singleton(TOPIC);
        Set<String> topics = new HashSet<String>();
        topics.add(TOPIC);
        JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc, String.class,
                String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

        JavaDStream<String> inputStream = directKafkaStream.map((Tuple2<String, String> t) -> t._2);
        inputStream.print();

        //directKafkaStream.dstream().saveAsTextFiles(targetFolderPath, "");
        //directKafkaStream.print();
        //directKafkaStream.filter(r -> r._2().length() > 0).print();

//        directKafkaStream.filter(new Function<Tuple2<String,String>, Boolean>() {
//            @Override
//            public Boolean call(Tuple2<String, String> arg0) throws Exception {
//                return arg0._2.length() > 0;
//            }
//        }).print();


//        directKafkaStream.foreachRDD(rdd -> {
//            if(rdd.count() > 0) {
//                rdd.saveAsHadoopFile(targetFolderPath, String.class, String.class, TextOutputFormat.class);;
//            }
//        });


        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
