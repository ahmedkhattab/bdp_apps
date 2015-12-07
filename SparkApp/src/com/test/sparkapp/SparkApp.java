package com.test.sparkapp;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
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

		UserGroupInformation ugi = UserGroupInformation.createRemoteUser("hdfs");
		try {
			ugi.doAs(new PrivilegedExceptionAction<Void>() {

				public Void run() throws Exception {
					String namenode = String.format("hdfs://%s:8020", System.getenv("NAMENODE_SERVICE_HOST"));

					Configuration configuration = new Configuration();

					FileSystem hdfs = FileSystem.get(new URI(namenode), configuration);

					String rabbitmq_address = String.format("tcp://%s:%s", System.getenv("RABBITMQ_SERVICE_HOST"),
							System.getenv("RABBITMQ_SERVICE_PORT_MQTT"));
					System.out.println("connecting to" + rabbitmq_address);
					SparkConf conf = new SparkConf().setAppName("testApp").setIfMissing("spark.master", "local[*]");
					JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(1000));
					JavaReceiverInputDStream<String> receiverStream = MQTTUtils.createStream(ssc, rabbitmq_address,
							"s1", org.apache.spark.storage.StorageLevel.MEMORY_ONLY());
					receiverStream.foreachRDD(new Function2<JavaRDD<String>, Time, Void>() {
						@Override
						public Void call(JavaRDD<String> rdd, Time time) {
							if (!rdd.isEmpty())
								rdd.saveAsTextFile(
										String.format("hdfs://%s:8020", System.getenv("NAMENODE_SERVICE_HOST"))
												+ "/user/hdfs/output/");
							return null;

						}
					});
					ssc.start();
					ssc.awaitTermination();
					ssc.stop();
					return null;
				}
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}