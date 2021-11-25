package com.venkat.kafka;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

public class ElasticSearchConsumer {

    public static void main(String[] args) {



    }

    private static RestHighLevelClient createElkClient () {

        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(AppPropertyReader.getValue("elastic.username"), AppPropertyReader.getValue("elastic.password")));
        RestClientBuilder clientBuilder = RestClient.builder(new HttpHost(AppPropertyReader.getValue("elastic.host"), 443, "https")).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
        });

        return new RestHighLevelClient(clientBuilder);

    }
}