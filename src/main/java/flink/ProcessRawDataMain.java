package flink;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProcessRawDataMain {

	public static final String INPUT_TOPIC = "raw-data";
	public static final String OUTPUT_TOPIC = "processed-data";
	public static final String KAFKA_SERVER = "localhost:9092";

	public static void main(String[] args) throws Exception {
		log.info(":: insiode main() ::");
		StreamRawDataConsumrer(INPUT_TOPIC, KAFKA_SERVER);
		//StreamProcessedDataConsumrer(OUTPUT_TOPIC, KAFKA_SERVER);
	}

	/**
	 * 
	 * @param inputTopic
	 * @param server
	 * @throws Exception
	 */
	public static void StreamRawDataConsumrer(String inputTopic, String server) throws Exception {
		log.info(":: insiode StreamRawDataConsumrer() ::");
		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		
		
		FlinkKafkaConsumer011<String> flinkKafkaConsumer = createStringConsumerForTopic(inputTopic, server);
		DataStream<String> stringInputStream = environment.addSource(flinkKafkaConsumer);

		FlinkKafkaProducer011<String> flinkKafkaProducer = createStringProducer(OUTPUT_TOPIC, KAFKA_SERVER);

		log.info(":: Processing the Raw Data and Keeping it in another Kafka TOpic : Start ::");
		stringInputStream.map(new ProcessConsumerData()).addSink(flinkKafkaProducer);
		log.info(":: Processing the Raw Data and Keeping it in another Kafka TOpic : Completed ::");

		environment.execute();
		log.info(":: Exiting StreamRawDataConsumrer() ::");
		
	}
	
	/**
	 * 
	 * @param inputTopic
	 * @param server
	 * @throws Exception
	 */
//	public static void StreamProcessedDataConsumrer(String inputTopic, String server) throws Exception {
//		log.info(":: insiode StreamProcessedDataConsumrer() ::");
//		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
//				
//		FlinkKafkaConsumer011<String> flinkKafkaConsumer = createStringConsumerForTopic(inputTopic, server);
//		DataStream<String> stringInputStream = environment.addSource(flinkKafkaConsumer);
//		
//		log.info(":: Writing dat to the Click House DB : Start ::");
//		stringInputStream.map(new ClickHouseConsumer());
//		log.info(":: Writing dat to the Click House DB : Completed ::");
//
//		environment.execute();
//		log.info(":: Exiting StreamProcessedDataConsumrer() ::");
//		
//	}

	/***
	 * 
	 * @param topic
	 * @param kafkaAddress
	 * @return
	 */
	public static FlinkKafkaProducer011<String> createStringProducer(String topic, String kafkaAddress) {
		log.info(":: insiode createStringProducer() ::");
		return new FlinkKafkaProducer011<>(kafkaAddress, topic, new SimpleStringSchema());
	}

	/**
	 * 
	 * @param topic
	 * @param kafkaAddress
	 * @return
	 */
	public static FlinkKafkaConsumer011<String> createStringConsumerForTopic(String topic, String kafkaAddress) {
		log.info(":: insiode createStringConsumerForTopic() ::");
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", kafkaAddress);
		// props.setProperty("group.id",kafkaGroup);
		FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), props);

		return consumer;
	}
}
