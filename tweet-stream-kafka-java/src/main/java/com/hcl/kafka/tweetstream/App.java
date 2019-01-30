package com.hcl.kafka.tweetstream;

import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import twitter4j.FilterQuery;
import twitter4j.HashtagEntity;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

/**
 * Hello world!
 *
 */
public class App {
	final static String consumerKey = "VXDJK9JgniluPQouRDkuFMbhf"; //API Key
	final static String consumerSecret = "K1KmMQiJJjaTDC8DBM1N9LXDGjGOFnCqKJba0fwwHIhVJSa5iZ"; //API Secret Key
	final static String accessToken = "2379836996-VL2Sicxih1THqTcXI9nXHLIwTGgbCOFBqvbFMWz"; //Access Token
	final static String accessSecret = "4HkAHQdSUUmsc6sj9HHKHt2XqB4mJpk6bPhGKNumTntLc"; //Access Token secret
	
	final static String keywords = "seahawks";
	final static String topicName = "seahawksTopic";
	
    public static void main( String[] args ) throws InterruptedException {
        System.out.println( "Hello World!" );
        final LinkedBlockingQueue<Status> queue = new LinkedBlockingQueue<Status>(1000);
        
    	Twitter twitter = TwitterFactory.getSingleton();
        
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
        	.setOAuthConsumerKey(consumerKey)
        	.setOAuthConsumerSecret(consumerSecret)
        	.setOAuthAccessToken(accessToken)
        	.setOAuthAccessTokenSecret(accessSecret);

        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        twitter.setOAuthConsumer(consumerKey, consumerSecret);
        twitter.setOAuthAccessToken(new AccessToken(accessToken, accessSecret));
        
        StatusListener listener = new StatusListener() {
			
			public void onException(Exception ex) {
				// TODO Auto-generated method stub
				ex.printStackTrace();			
			}
			
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				// TODO Auto-generated method stub			
			}
			
			public void onStatus(Status status) {
				queue.offer(status);
				// TODO Auto-generated method stub	
			}
			
			public void onStallWarning(StallWarning warning) {
				// TODO Auto-generated method stub			
			}
			
			public void onScrubGeo(long userId, long upToStatusId) {
				// TODO Auto-generated method stub			
			}
			
			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
				// TODO Auto-generated method stub			
			}
			
        };
       
        System.out.println(twitterStream.getAuthorization());
        //Test
        twitterStream.addListener(listener);
        // sample() method internally creates a thread which manipulates TwitterStream 
        // and calls these adequate listener methods continuously.
        //twitterStream.sample();
        
        //You can search for Tweets using Query class and Twitter.search(twitter4j.Query) method.
        FilterQuery query = new FilterQuery().track(keywords);
        twitterStream.filter(query);
        
        /*try {
			QueryResult result = twitter.search(query);
			for(Status status: result.getTweets()) {
//				System.out.println("@" + status.getUser().getScreenName() + ":" +status.getText());	
				System.out.println(status.getText());
			}
		} catch (TwitterException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
        //System.out.println("What's happening?");
        Thread.sleep(2000);
        // Add Kafka producer config settings
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,  "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,  "org.apache.kafka.common.serialization.StringSerializer");
        System.out.println("It breaks at producer!");
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        System.out.println("It breaks when creating future");
        Future<RecordMetadata> future = producer.send(new ProducerRecord<String, String>(topicName, "5", "This finally works!	"));
        
        System.out.println("Checking the connection now...");
        if(future.isCancelled()) {
        	System.out.println("Something got dropped!");
        }
        
        Thread.sleep(5000);
        	
        System.out.println(future.isDone());
        
        
        int i = 0;
        int j = 0;
        
        //System.out.println(ret.getHashtagEntities());
        
        while(i < 100) {
        	Status ret =  queue.poll();
        	
        	if (ret == null) {
            	System.out.println("Waiting for response..");
        		Thread.sleep(5000);
        		i++;
        	}else {
                System.out.println(ret.getText());
                producer.send(new ProducerRecord<String, String>(
                        topicName, Integer.toString(j++), ret.getText()
                ));
        		/*for(HashtagEntity hashtage : ret.getHashtagEntities()) {
                	System.out.println(ret.getHashtagEntities().length);
        			System.out.println("Hashtag: "+ hashtage.getText());
        			producer.send(new ProducerRecord<String, String>(
        				topicName, Integer.toString(j++), hashtage.getText()));
        		}*/
        	}
        }
             
        producer.close();
        Thread.sleep(5000);
        twitterStream.shutdown();
        
    }
}
