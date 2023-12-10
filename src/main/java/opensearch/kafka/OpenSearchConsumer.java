package opensearch.kafka;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {

    public static RestHighLevelClient createOpenSearchClient(){
        BonsaiConfig bc = new BonsaiConfig("src/main/resources/bonsai_config.txt");
        String connString = bc.getConnString();

        URI connUri = URI.create(connString);
        String[] auth = connUri.getUserInfo().split(":");

        CredentialsProvider cp = new BasicCredentialsProvider();
        cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

        RestHighLevelClient rhlc = new RestHighLevelClient(
                RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                        .setHttpClientConfigCallback(
                                httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                        .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));

        return rhlc;
    }

    private static KafkaConsumer<String, String> createKafkaConsumer(String groupID){
        // custom class for reading in data from Kafka config file
        KafkaConfig kc = new KafkaConfig("src/main/resources/kafka_config.txt");

        //  connect to upstash server
        var props = new Properties();
        props.put("bootstrap.servers", kc.getBootstrapServer());
        props.put("sasl.mechanism", kc.getSaslMechanism());
        props.put("security.protocol", kc.getSecurityProtocol());
        props.put("sasl.jaas.config", kc.getSaslJaasConfig());

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", groupID);
        props.put("auto.offset.reset", "earliest");

        return new KafkaConsumer<>(props);
    }

    private static String extractID(String json){
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    public static void main(String[] args) throws IOException {
        String groupID = "consumer-openSearch";
        String topic = "wikimedia.recentchange";

        //Create logger
        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

        //Create open search client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        // Create Kafka consumer
        KafkaConsumer<String, String> openSearchConsumer = createKafkaConsumer(groupID);

        //Create index if does not exist. Try block will close openSearch client/consumer on success/fail
        try(openSearchClient; openSearchConsumer) {
            String wikimediaIndex = "wikimedia";
            Boolean indexExists = openSearchClient.indices()
                    .exists( new GetIndexRequest(wikimediaIndex), RequestOptions.DEFAULT);
            if(!indexExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest(wikimediaIndex);
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("Wikimedia index created");
            } else {
                log.info("Wikimedia index already exists");
            }

            // get reference to main thread
            final Thread mainThread = Thread.currentThread();

            //add shutdown hook, will throw exception within while loop
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    log.info("Shutdown detected, exiting with consumer.wakeup()");
                    openSearchConsumer.wakeup();
                    //join main thread to allow execution of its code
                    try {
                        mainThread.join();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });

            try {
                //subscribe to topic
                openSearchConsumer.subscribe(Collections.singleton(topic));

                //poll for data
                while (true) {
                    log.info("Polling");
                    ConsumerRecords<String, String> records = openSearchConsumer.poll(Duration.ofMillis(3000));

                    int recordCount = records.count();
                    log.info("Received " + recordCount + " record(s)");

                    // upload data to openSearch
                    for(ConsumerRecord record: records){
                        try {
                            // create ID based on wikimedia record for idempotent insertions
                            String id = extractID(record.value().toString());

                            // create upload request
                            IndexRequest indexRequest = new IndexRequest("wikimedia")
                                    .source(record.value(), XContentType.JSON)
                                    .id(id);
                            // upload record and save response
                            IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
                            // log uploaded record ID
                            log.info(response.getId());
                        } catch(Exception e){
                            // some records have mapping depth > 20 which openSearch does not allow, simply ignore them and process next message
                            log.info("JSON record mapping depth > 20");
                        }
                    }
                }
            } catch(WakeupException we) {
                log.info("Consumer shutting down");
            } catch(Exception e){
                log.error("Unexpected consumer error: ", e);
            } finally {
                //close consumer and commit offsets
                openSearchConsumer.close();
            }
        }
    }
}
