/**
 * 
 */
package com.github.trantor_si.linkedin_apache_kafka.tutorial1;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import javax.management.timer.Timer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author luccostajr
 * 
 * Consumer class to test receive messages to Kafka broker
 *
 */
public class ConsumerDemo {
	public enum PolicyType { SUBSCRIBE, SEEK }
	private PolicyType policyType = PolicyType.SUBSCRIBE;
	private Random rand = SecureRandom.getInstanceStrong();  // SecureRandom is preferred to Random
	private static final int DEFAULT_NUMBER_OF_PARTITIONS = 3;
	private static final String DEFAULT_TOPIC = "trantor_topic";
	private static final String DEFAULT_BOOTSTRAP_SERVER = "localhost:9092";
	private static final String DEFAULT_GROUP_ID = "trantor-application";
	private static final String DEFAULT_RESET_CONFIG = "earliest"; /*earliest/latest/none*/
	private Properties properties = new Properties();
	private KafkaConsumer<String,String> kafkaConsumer = null;
	public static final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
	private TopicPartition defaultTopicPartition = null;
	
	/**
	 * Receive a message to Kafka Broker
	 * 
	 * @param groupID Kafka message group identification
	 * @param messageTopic Kafka message topic
	 * @param messageKey Kafka message Key
	 * @param messageText Kafka message text to be sent
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public void receiveMessages ( ) {
		ConsumerRecords<String, String> consumerRecords =
				kafkaConsumer.poll( Duration.ofSeconds( 5 )) ; // new in Kafka 2.0.0
		
		for ( ConsumerRecord<String,String> consumerRecord : consumerRecords ) {
			logger.info(
				String.format("%nKey: %s%nValue:%s%nPartition: %d%nOffset: %d%n", 
					consumerRecord.key(), 
					consumerRecord.value(), 
					consumerRecord.partition(), 
					consumerRecord.offset() 
				) 
			);
		}
	}
	
	/**
	 * Receive a message to Kafka Broker
	 * 
	 * @param groupID Kafka message group identification
	 * @param messageTopic Kafka message topic
	 * @param messageKey Kafka message Key
	 * @param messageText Kafka message text to be sent
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public Boolean receiveMessages ( int numberOfMessagesReadSoFar, int numberOfMessagesToRead ) {
		ConsumerRecords<String, String> consumerRecords =
				kafkaConsumer.poll( Duration.ofSeconds( 5 )) ; // new in Kafka 2.0.0
		
		for ( ConsumerRecord<String,String> consumerRecord : consumerRecords ) {
			logger.info(
				String.format("%nKey: %s%nValue:%s%nPartition: %d%nOffset: %d%nRead so far: %d%n", 
					consumerRecord.key(), 
					consumerRecord.value(), 
					consumerRecord.partition(), 
					consumerRecord.offset(),
					++numberOfMessagesReadSoFar
				) 
			);
			
			if ( numberOfMessagesReadSoFar >= numberOfMessagesToRead ) {
				logger.info(
					String.format("%n[receiveMessages]: End of message receive due reach the limit: %d%n", numberOfMessagesToRead )
				);
				return false;
			}
		}
		
		return true;
	}
	
	/**
	 * 
	 * @author luccostajr
	 *
	 */
	public class ConsumerRunnable implements Runnable {

		private CountDownLatch latch = null;
		private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);
		public ConsumerRunnable (CountDownLatch latch ) {
			this.latch = latch;
		}
		
		public void run( ) {
			try {
				if ( policyType == PolicyType.SUBSCRIBE ) {
					while ( true ) {
						Thread.sleep (Timer.ONE_SECOND);
						receiveMessages ( );
					}
				}
				else
				if ( policyType == PolicyType.SEEK ) {
					int numberOfMessagesToRead = 5;
					int numberOfMessagesReadSoFar = 0;
					boolean keepOnReading = true;

					long offsetToReadFrom = 15L;
					kafkaConsumer.seek(defaultTopicPartition, offsetToReadFrom);

					while ( keepOnReading ) {
						Thread.sleep (Timer.ONE_SECOND);
						keepOnReading = receiveMessages ( numberOfMessagesReadSoFar, numberOfMessagesToRead );
					}
				}
				else {
					logger.error(String.format("%n[ConsumerRunnable]: Unknown Policy type: [%s]!%n"), policyType.toString());
				}
			}
			catch (WakeupException e) {
				logger.error(String.format("%n[ConsumerRunnable]::[WakeUpException]: received shutdown signal!%n"));
			} 
			catch (InterruptedException e) {
				logger.error(String.format("%n[ConsumerRunnable]::[InterruptedException]: Sleep failure!%n"));
			}
			finally {
				logger.info(String.format("%n[ConsumerRunnable]: Exiting the consumer application.%n"));
				closeThread();
			}
		}
		
		public void closeThread ( ) {
			closeKafkaConsumer ();

			// tell our main code we're done with the consumer
			latch.countDown();
		}
		
		public void shutdown () {
			// the wakeup method is a special method to interrupt consumer.poll()
			// it will throw the exception WakeUpException 
			kafkaConsumer.wakeup();
		}
		
	}
	
	/**
	 * Flush the messages and close Kafka producer (OK) 
	 * 
	 */
	public void closeKafkaConsumer ( ) {
		// close producer
		kafkaConsumer.close();
	}

	/**
	 * Convert Exception message in String (OK)
	 * 
	 * @param exception Exception
	 * @return String
	 */
	public static String getExceptionMessage(Exception exception) {
		String result = "";
		StackTraceElement[] stes = exception.getStackTrace();
		
		for (int i = 0; i < stes.length; i++) {
			result = String.format( "%s%s.%s line: %d%n",  
					result, stes[i].getClassName(), stes[i].getMethodName(), stes[i].getLineNumber());
		}
		
		return result;
	}

	/**
	 * Create producer properties (OK)
	 * 
	 * @param bootstrapServer Location of Kafka Bootstrap, as "localhost:9092" (default)
	 * @param groupID 
	 * @param resetConfig 
	 * @return Properties
	 */
	private Properties createProperties ( String bootstrapServer, String groupID, String resetConfig ) {
		properties.setProperty( ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty( ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName() );
		properties.setProperty( ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName() );
		properties.setProperty( ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, resetConfig );

		if ( ( groupID != null ) && ( groupID.length() > 0 ) ) {
			properties.setProperty( ConsumerConfig.GROUP_ID_CONFIG, groupID );
		}
		
		
		return properties;
	}
	
	/**
	 * Create the kafka consumer (OK)
	 * 
	 * @param properties Properties object containing all Kafka Producer parameters
	 * @return KafkaProducer The created Kafka Producer object
	 */
	private KafkaConsumer<String,String> createKafkaConsumer ( Properties properties ) {
		return new KafkaConsumer<String, String>(properties);
	}
	
	/**
	 * subscribe topics to kafka consumer (OK)
	 * 
	 * @param topicsArray
	 */
	private void subscribeConsumerTopics ( String[] topicsArray ) {
		if ( policyType == PolicyType.SUBSCRIBE ) {
			if ( topicsArray.length > 0) {
				List<String> topicsList = new ArrayList<String>();
				Collections.addAll(topicsList, topicsArray);

				kafkaConsumer.subscribe(topicsList);
			}
			else {
				kafkaConsumer.subscribe(Arrays.asList(DEFAULT_TOPIC));
			}
		}
		else 
		if ( policyType == PolicyType.SEEK ) {
			if ( topicsArray.length > 0) {
				ArrayList<TopicPartition> partitionToReadFromList = new ArrayList<TopicPartition>();
	
				// assign and seek are mostly used to replay data or fetch a specific message
				for ( int i = 0; i < topicsArray.length; i++ ) {
					TopicPartition partitionToReadFrom = new TopicPartition( 
							topicsArray[i], rand.nextInt(DEFAULT_NUMBER_OF_PARTITIONS) );
					partitionToReadFromList.add(partitionToReadFrom);
				}

				// create a unique TopicPartition, the first one
				defaultTopicPartition = partitionToReadFromList.get(0);
				
				// assign
				kafkaConsumer.assign(partitionToReadFromList);
			}
			else {
				// create a unique TopicPartition, the default
				defaultTopicPartition = new TopicPartition( DEFAULT_TOPIC, 0 );
				
				// assign
				kafkaConsumer.assign(Arrays.asList(defaultTopicPartition));
			}
		}
	}
	
	public void runSingleThread ( ) {
		// latch for dealing with multiple threads
		CountDownLatch latch = new CountDownLatch(1);
		
		// create the consumer runnable
		logger.info(String.format("%n[runSingleThread]: Creating the consumer thread...%n"));
		final Runnable consumerRunnable = new ConsumerRunnable(latch);
		Thread producerThread = new Thread(consumerRunnable);
		
		// start the thead
		logger.info(String.format("%n[runSingleThread]: Starting thread...%n"));
		producerThread.start();
		
		// add a shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread( () -> {
			logger.info(String.format("%n[runSingleThread]: Caught shutdown hook.%n"));
			((ConsumerRunnable) consumerRunnable).shutdown();
			
			try {
				latch.await();
			} 
			catch (InterruptedException e) {
				logger.info(String.format("%n[runSingleThread]: Application got interrupted.%n"), e);
			}
			
			logger.info(String.format("%n[runSingleThread]: Application has exit.%n"));
		}));
		
		try {
			latch.await();
		} 
		catch (InterruptedException e) {
			logger.info(String.format("%n[runSingleThread]: Application got interrupted.%n"), e);
		}
		finally {
			logger.info(String.format("%n[runSingleThread]: Application is closing...%n"));
		}
	}
	
	/**
	 * Constructor (OK)
	 * 
	 * @param bootstrapServer
	 * @param groupID
	 * @param topicsArray
	 * @param resetConfig
	 * @throws Exception
	 */
	public ConsumerDemo( 
			String bootstrapServer, 
			String groupID, 
			String[] topicsArray, 
			String resetConfig,
			PolicyType... policyType
	) throws Exception {
		if ( ( bootstrapServer == null ) || ( bootstrapServer.length() == 0 ) ) {
			bootstrapServer = DEFAULT_BOOTSTRAP_SERVER;
		}
		
		if ( ( groupID == null ) || ( groupID.length() == 0 ) ) {
			groupID = DEFAULT_GROUP_ID;
		}
		
		if ( ( resetConfig == null ) || ( resetConfig.length() == 0 ) ) {
			resetConfig = DEFAULT_RESET_CONFIG;
		}
		
		if ( policyType.length > 0 ) {
			this.policyType = policyType[0];
		}
		
		properties = createProperties(bootstrapServer, groupID, resetConfig);
		kafkaConsumer = createKafkaConsumer ( properties );
		
		// subscribe consumer to topic(s)
		subscribeConsumerTopics ( topicsArray );
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			String[] topicsArray = { DEFAULT_TOPIC };

			// create a ConsumerDemo object and run a thread
			ConsumerDemo consumerDemo = new ConsumerDemo (
				DEFAULT_BOOTSTRAP_SERVER, 
				DEFAULT_GROUP_ID, topicsArray, 
				DEFAULT_RESET_CONFIG,
				PolicyType.SEEK
			);
			consumerDemo.runSingleThread();
		} catch (Exception e) {
			logger.error ( "[ConsumerDemo::main]: ", e );
		}
	}
}
