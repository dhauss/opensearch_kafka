package opensearch.kafka;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

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

    public static void main(String[] args) throws IOException {
        //Create open search client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        //Create logger
        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

        //Create index if does not exist. Try block will close openSearch client on success/fail
        try(openSearchClient) {
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
        }
    }
}
