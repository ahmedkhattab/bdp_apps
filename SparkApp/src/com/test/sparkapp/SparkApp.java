package com.test.sparkapp;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.mqtt.MQTTUtils;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkApp {
	public static void main(String[] args) {
		
		final String namenode = String.format("hdfs://%s:50070", System.getenv("NAMENODE_SERVICE_HOST"));
		
		Configuration configuration = new Configuration();
		try {
			FileSystem hdfs = FileSystem.get( new URI(
					namenode), configuration );
		} catch (IOException | URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
				
		String rabbitmq_address = String.format("tcp://%s:%s", System.getenv("RABBITMQ_SERVICE_HOST"),
				System.getenv("RABBITMQ_SERVICE_PORT_MQTT"));
		System.out.println("connecting to" + rabbitmq_address);
		SparkConf conf = new SparkConf().setAppName("testApp").setIfMissing("spark.master", "local[*]");
		JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(1000));
		JavaReceiverInputDStream<String> receiverStream = MQTTUtils.createStream(ssc, rabbitmq_address, "s1",
				org.apache.spark.storage.StorageLevel.MEMORY_ONLY());
		receiverStream.foreachRDD( new Function2<JavaRDD<String>, Time, Void>() {
		      @Override
		      public Void call(JavaRDD<String> rdd, Time time) {
		    	  rdd.saveAsTextFile(namenode + "/user/hdfs/output.txt");

		    	  return null;
		    
		      }
		    });
		ssc.start();
		ssc.awaitTermination();
		ssc.stop();
	}
}