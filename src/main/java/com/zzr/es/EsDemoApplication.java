package com.zzr.es;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class EsDemoApplication implements CommandLineRunner{


	public static void main(String[] args) {
		SpringApplication.run(EsDemoApplication.class, args);
	}

	@Override
	public void run(String... strings) throws Exception {
		RestClient restClient = RestClient.builder(new HttpHost("localhost",9200)).build();

	}
}
