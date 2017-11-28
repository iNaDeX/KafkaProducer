import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class KafkaProducer {
    private static final String BROKER_LIST = "kafka.broker.list";
    private static final String CONSUMER_KEY = "consumerKey";
    private static final String CONSUMER_SECRET = "consumerSecret";
    private static final String TOKEN = "accessToken";
    private static final String SECRET = "accessTokenSecret";
    private static final String KAFKA_TOPIC = "kafka.twitter.raw.topic";

    public static void run(Context context) throws InterruptedException {
        // Producer properties
        Properties properties = new Properties();
        properties.put("metadata.broker.list", context.getString(BROKER_LIST));
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("request.required.acks", "1");
        ProducerConfig producerConfig = new ProducerConfig(properties);

        final Producer<String, String> producer = new Producer<>(producerConfig);

        // Create an appropriately sized blocking queue
        BlockingQueue<String> queue = new LinkedBlockingQueue<>(10000);

        // Define our endpoint, with default value delimited=length (needed for our processor)
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.languages(Lists.newArrayList("en"));
        // TODO : read terms to track from the configuration file
        endpoint.trackTerms(Lists.newArrayList("microsoft", "#google"));

        Authentication auth = new OAuth1(context.getString(CONSUMER_KEY), context.getString(CONSUMER_SECRET), context.getString(TOKEN), context.getString(SECRET));

        // Create a new BasicClient. By default gzip is enabled.
        BasicClient client = new ClientBuilder()
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue))
                .build();

        // Establish a connection
        client.connect();

        while (!client.isDone()) {
            KeyedMessage<String, String> message = null;
            try {
                message = new KeyedMessage<>(context.getString(KAFKA_TOPIC), queue.take());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println(message); // print the message
            producer.send(message);
        }
        System.out.println("Client connection closed unexpectedly: " + client.getExitEvent().getMessage());
        producer.close();
        client.stop();
    }

    public static void main(String[] args) {
        if (args.length != 1){
            System.err.println("USAGE:\nKafkaProducer <configFilePath>");
            return;
        }
        try {
            String configFileLocation = args[0];
            Context context = new Context(configFileLocation);
            KafkaProducer.run(context);
        } catch (Exception e) {
            System.out.println(e.toString());
        }
    }
}