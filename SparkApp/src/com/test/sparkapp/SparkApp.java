package com.test.sparkapp;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.mqtt.MQTTUtils;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkApp {
public static void main(String[] args)
{

	String rabbitmq_address = String.format("tcp://%s:%s", System.getenv("RABBITMQ_SERVICE_HOST"), System.getenv("RABBITMQ_SERVICE_PORT_MQTT"));
	System.out.println("connecting to" + rabbitmq_address);
	SparkConf conf = new SparkConf().setAppName("testApp").setIfMissing("spark.master", "local[*]");
	JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(1000));
	JavaReceiverInputDStream<String> receiverStream = MQTTUtils.createStream(ssc, rabbitmq_address, "sensor1", org.apache.spark.storage.StorageLevel.MEMORY_ONLY());
	receiverStream.print();
    ssc.start();
    ssc.awaitTermination();
}
}
