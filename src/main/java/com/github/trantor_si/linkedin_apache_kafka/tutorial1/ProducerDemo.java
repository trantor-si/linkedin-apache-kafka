/**
 * 
 */
package com.github.trantor_si.linkedin_apache_kafka.tutorial1;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import javax.management.timer.Timer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * @author luccostajr
 * 
 * Producer class to test send messages to Kafka broker
 *
 */
public class ProducerDemo {
	private static final String BOOTSTRAP_DEFAULT_SERVER = "localhost:9092";
	private Properties properties = new Properties();
	private KafkaProducer<String,String> kafkaProducer = null;
	public static final Logger logger = LoggerFactory.getLogger(ProducerDemo.class);
	/**
	 * Convert Exception message in String
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
	 * Create producer properties
	 * 
	 * @param bootstrapServer Location of Kafka Bootstrap, as "localhost:9092" (default)
	 * @return Properties
	 */
	private Properties createProperties ( String bootstrapServer ) {
		properties.setProperty( ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty( ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName() );
		properties.setProperty( ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName() );
		
		return properties;
	}
	
	/**
	 * Create the kafka producer
	 * 
	 * @param properties Properties object containing all Kafka Producer parameters
	 * @return KafkaProducer The created Kafka Producer object
	 */
	private KafkaProducer<String,String> createKafkaProducer ( Properties properties ) {
		return new KafkaProducer<String, String>(properties);
	}
	
	/**
	 * Create a producer record
	 * 
	 * @param messageTopic Kafka message topic
	 * @param messageKey Kafka message Key
	 * @param messageText Kafka message text to be sent
	 * @return ProducerRecord The created Producer Record object
	 */
	private ProducerRecord<String, String> createProducerRecord ( String messageTopic, String messageKey, String messageText ) {
		return new ProducerRecord<String, String> ( messageTopic, messageKey, messageText );
	}
	
	
	/**
	 * Send a message to Kafka Broker
	 * 
	 * @param messageTopic Kafka message topic
	 * @param messageKey Kafka message Key
	 * @param messageText Kafka message text to be sent
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public void sendMessage (String messageTopic, final String messageKey, String messageText ) throws InterruptedException, ExecutionException {
		// create a producer record
		ProducerRecord<String, String> producerRecord = createProducerRecord(messageTopic,messageKey,messageText);
		
		// send data
		kafkaProducer.send(producerRecord, new Callback() {
			
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				// executed every time a record is successfully sent or an exception is thrown
				if ( exception == null ) {
					// the record was successfully sent
					logger.info(
							String.format (
								"%nReceived new metadata:%nKey: %s%nTopic: %s%nPartition: %s%nOffset: %s%nTimestamp: %s%n[%s]%n",
								messageKey, metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp(), metadata.toString()
							)
					);
				} else {
					logger.error ("[ConsumerDemo::sendMessage]: Error while producing.");
				}
			}
		}).get(); // block the .send() to make it synchronous - don't do this in production!!!
	}
	
	/**
	 * Flush all the messages data
	 * 
	 */
	public void flush ( ) {
		kafkaProducer.flush();
	}
	
	/**
	 * Flush the messages and close Kafka producer 
	 * 
	 */
	public void close() {
		// flush data
		kafkaProducer.flush();
		
		// close producer
		kafkaProducer.close();
	}
	
	public ProducerDemo( String bootstrapServer ) throws Exception {
		if ( ( bootstrapServer == null ) || ( bootstrapServer.length() == 0 ) ) {
			bootstrapServer = BOOTSTRAP_DEFAULT_SERVER;
		}
		
		properties = createProperties(bootstrapServer);
		kafkaProducer = createKafkaProducer ( properties );
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			// create a ConsumerDemo object
			ProducerDemo producerDemo = new ProducerDemo (null);
			
			int i = 0;
			try {
				while ( true ) {
					String messageTopic = "trantor_topic";
					String messageKey = "id_" + Integer.toString(++i);
					String messageText = "[Trantor SI] Cycle: " + Integer.toString(i);
					
					// send a message to kafka broker
					producerDemo.sendMessage ( messageTopic, messageKey, messageText );
					
					//Thread.sleep (Timer.ONE_SECOND);
				}
			}
			finally {
				// close producer
				producerDemo.close();
			}
		} catch (Exception e) {
			logger.error ( "[ConsumerDemo::main]: ", e );
		}
	}

}
