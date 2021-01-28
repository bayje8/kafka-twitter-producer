package com.kafka.twitter;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class KafkaTwitterProducer {

	Logger logger = LoggerFactory.getLogger(KafkaTwitterProducer.class);
	
	public String apiKey = "EDvVJBnuAsxCSByzlUrnaPkz2";
	public String apiKeySecret = "ApbrlndC4mxxooSrWPFWycMykXkuyj4ZsT8w0Fi26QQXdjWUGO";
	public String accessToken = "799814150-qL8Y47RkyHbNIJ9azDsN2Lmlw81KhsU0Bqsqs4Aa";
	public String accessTokenSecret = "F7KQmT4l1v4cmZAaZ6uqOSmieXf8JihKBDehWt6kSUx5J";

	public KafkaTwitterProducer() {
	}

	public static void main(String[] args) {
		new KafkaTwitterProducer().run();
	}

	public void run() {
		//Kafka producer properties
		Properties producerProps = new Properties();
		producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		//properties for safe producer
		producerProps.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		producerProps.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		producerProps.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
		producerProps.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");//since it is kafka > 1.1, otherwise value should be set to 1
		
		//properties for high throughput producer (with compromising latency and cpu usage
		//Here messages are collected for certain period (linger ms) and sent as a batch
		//the batch of messages are compressed in snappy compression mode
		//One more setting, the default max batch size is 16KB, but we try to set it to 32KB 
		producerProps.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
		producerProps.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		producerProps.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
		
		
		
		//Create Kakfa producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProps);
		
		/**
		 * Set up your blocking queues: Be sure to size these properly based on expected
		 * TPS of your stream
		 */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
		
		// create twitter client
		Client client = createTwitterClient(msgQueue);
		// connect to client
		logger.info("connecting to hose bird client");
		client.connect();
		// poll for messages from twitter client
		logger.info("Poll for messages");
		while (!client.isDone()) {
			String msg = msgQueue.poll();
			if (msg != null) {
				logger.info(msg);
				// send messages to kafka producer
				producer.send(new ProducerRecord<String, String>("twitter", null, msg), new Callback() {					
					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						if(exception != null) {
							logger.error("Something bad happended", exception);
						}
					}
				});
				
				
			}
		}

		client.stop();
	}

	public Client createTwitterClient(BlockingQueue<String> msgQueue) {
		logger.info("Creating twitter client with hosebird library..");
		// Declaring https://stream.twitter.com as our host
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Look for hashtags #Defi
		List<String> terms = Lists.newArrayList("Defi","usa","biden","kamala","bitcoin");
		hosebirdEndpoint.trackTerms(terms);

		// Use the OAuth Credentials generated for the app created in developer.twitter.com 
		Authentication hosebirdAuth = new OAuth1(apiKey, apiKeySecret, accessToken, accessTokenSecret);

		ClientBuilder builder = new ClientBuilder()
									.name("Hosebird-Client-01") // name of the client
									.hosts(hosebirdHosts)
									.authentication(hosebirdAuth)
									.endpoint(hosebirdEndpoint)
									.processor(new StringDelimitedProcessor(msgQueue));

		//build the client
		Client hosebirdClient = builder.build();
		return hosebirdClient;
	}

}
