package com.test.sparkapp;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import scala.Tuple2;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

public final class SparkWordCount {
	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(final String[] args) throws Exception {
		try {
			UserGroupInformation ugi = UserGroupInformation.createRemoteUser("hdfs");

			ugi.doAs(new PrivilegedExceptionAction<Void>() {

				public Void run() throws Exception {
					SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount");
					JavaSparkContext ctx = new JavaSparkContext(sparkConf);
					String namenode = String.format("hdfs://%s:8020", System.getenv("NAMENODE_SERVICE_HOST"));
					// create Spark context with Spark configuration

					// get threshold
					final int threshold = Integer.parseInt(args[1]);

					// read in text file and split each document into words
					JavaRDD<String> tokenized = ctx.textFile(namenode + args[0])
							.flatMap(new FlatMapFunction<String, String>() {
						public Iterable call(String s) {
							return Arrays.asList(s.split(" "));
						}
					});

					// count the occurrence of each word
					JavaPairRDD<String, Integer> counts = tokenized
							.mapToPair(new PairFunction<String, String, Integer>() {
						public Tuple2 call(String s) {
							return new Tuple2(s, 1);
						}
					}).reduceByKey(new Function2<Integer, Integer, Integer>() {
						public Integer call(Integer i1, Integer i2) {
							return i1 + i2;
						}
					});

					// filter out words with fewer than threshold occurrences
					JavaPairRDD<String, Integer> filtered = counts
							.filter(new Function<Tuple2<String, Integer>, Boolean>() {
						public Boolean call(Tuple2<String, Integer> tup) {
							return tup._2 >= threshold;
						}
					});

					// count characters
					JavaPairRDD<Character, Integer> charCounts = filtered
							.flatMap(new FlatMapFunction<Tuple2<String, Integer>, Character>() {
						@Override
						public Iterable<Character> call(Tuple2<String, Integer> s) {
							Collection<Character> chars = new ArrayList<Character>(s._1().length());
							for (char c : s._1().toCharArray()) {
								chars.add(c);
							}
							return chars;
						}
					}).mapToPair(new PairFunction<Character, Character, Integer>() {
						@Override
						public Tuple2<Character, Integer> call(Character c) {
							return new Tuple2<Character, Integer>(c, 1);
						}
					}).reduceByKey(new Function2<Integer, Integer, Integer>() {
						@Override
						public Integer call(Integer i1, Integer i2) {
							return i1 + i2;
						}
					});

					System.out.println(charCounts.collect());
					charCounts.saveAsTextFile(namenode + args[0] + "_output");
					return null;

				}
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
