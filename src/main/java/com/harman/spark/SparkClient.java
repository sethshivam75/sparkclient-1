package com.harman.spark;

import java.util.ConcurrentModificationException;
import java.util.Timer;
import java.util.Vector;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.harman.dbinsertion.InsertIntoMongoDB;
import com.harman.dbinsertion.InsertionIntoMariaDB;
import com.mongodb.MongoClient;

public class SparkClient implements DBkeys {

	@SuppressWarnings("unused")
	private JavaStreamingContext ssc = null;
	static InsertionIntoMariaDB mInsertionIntoMariaDB;

	@SuppressWarnings("resource")
	public static void main(String[] args) {
		System.out.println("In main spark Client");
		SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("SmartAudioAnalytics");
		JavaSparkContext context = new JavaSparkContext(sparkConf);
		JavaStreamingContext ssc = new JavaStreamingContext(context, new Duration(30000));
		// TODO close ssc connection.
		JavaReceiverInputDStream<String> JsonReq = ssc.socketTextStream("localhost", 9997,
				StorageLevels.MEMORY_AND_DISK_SER);

		JsonReq.foreachRDD(new VoidFunction<JavaRDD<String>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaRDD<String> rdd) throws Exception {
				rdd.foreach(new VoidFunction<String>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public void call(String s) throws Exception {
						System.out.println(s);
						InsertIntoMongoDB.getInstance().openConnection();
						InsertIntoMongoDB.getInstance().inserSingleRecordMongoDB(s);
						InsertionIntoMariaDB.getInstance().insertIntoMariaDB(s);
					}

				});
			}
		});
		ssc.start();
		ssc.awaitTermination();
	}

}
