import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class MessagesProducer {

    public static String HOST = "sandbox-hdp.hortonworks.com:6667";
    public static String TOPIC = "booking_topic";
    public static BlockingQueue<String> QUEUE = new ArrayBlockingQueue<>(10);
    public static int KAFKA_PRODUCER_THREADS_COUNT = 3;

    public static void main(String[] args) throws InterruptedException {

        String sourceCsvPath = args[0];

        Thread readingFileAndFillingQueueThread = createReadingFileAndFillingQueueThread(QUEUE, sourceCsvPath);
        readingFileAndFillingQueueThread.start();

        Thread[] kafkaProducerThreads = new Thread[KAFKA_PRODUCER_THREADS_COUNT];
        for (int i = 0; i < kafkaProducerThreads.length; i++) {
            kafkaProducerThreads[i] = createKafkaProducerThread(HOST, TOPIC, QUEUE);
            kafkaProducerThreads[i].start();
        }

        while (readingFileAndFillingQueueThread.isAlive()) {
            Thread.sleep(1000);
        }
        while (QUEUE.size() > 0) {
            Thread.sleep(1000);
        }
        for (Thread thread : kafkaProducerThreads) {
            thread.isInterrupted();
        }
        System.exit(0);
    }

    public static Thread createReadingFileAndFillingQueueThread(BlockingQueue<String> queue, String sourceCsvPath) {
        Thread readingFileAndFillingQueueThread = new Thread(new Runnable() {
            @Override
            public void run() {
                String rawBeingPlacedToQueue = "";
                try {
                    FileReader fileReader = new FileReader(sourceCsvPath);
                    BufferedReader bufferedReader = new BufferedReader(fileReader);
                    bufferedReader.readLine();
                    while ((rawBeingPlacedToQueue = bufferedReader.readLine()) != null) {
                        System.out.println(rawBeingPlacedToQueue);
                        queue.put(rawBeingPlacedToQueue);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        return readingFileAndFillingQueueThread;
    }

    public static Thread createKafkaProducerThread(String kafkaBrokerHost, String kafkaTopic, BlockingQueue<String> queue) {
        Thread producerThread = new Thread(new Runnable() {
            @Override
            public void run() {
                Properties props = new Properties();
                props.put("bootstrap.servers", kafkaBrokerHost);
                props.put("acks", "all");
                props.put("request.required.acks", "1");
                props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

                Producer<String, String> producer = new KafkaProducer<>(props);
                String currentRow = "";
                while (true) {
                    if (Thread.currentThread().isInterrupted()) {
                        producer.close();
                        break;
                    }
                    try {
                        currentRow = queue.take();
                        producer.send(new ProducerRecord<String, String>(kafkaTopic, currentRow)).get();
                        System.out.println(currentRow);
                        producer.flush();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

        });
        return producerThread;
    }

}
