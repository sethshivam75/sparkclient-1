package com.harman.spark;

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
import com.harman.models.DBkeys;

public class SparkClient implements DBkeys {

	final static int emailAlertCounter = 4;
	@SuppressWarnings("unused")
	private JavaStreamingContext ssc = null;

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

				long count = rdd.count();

				rdd.foreach(new VoidFunction<String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public void call(String s) throws Exception {
						System.out.println(s);
						InsertIntoMongoDB insertMongo = InsertIntoMongoDB.getInstance();
						insertMongo.openConnection();
						insertMongo.updateCounter();
						insertMongo.inserSingleRecordMongoDB(s);

						InsertionIntoMariaDB insertMaria = InsertionIntoMariaDB.getInstance();
						insertMaria.insertIntoMariaDB(s);

						if (insertMongo.getCounter() >= count) {
							if (insertMaria.getFeatureCounter() > emailAlertCounter) {
								// send email
								SparkTriggerThread.SendEmail("CriticalTemperatureShutDown",
										insertMaria.getFeatureCounter());
							}
							insertMaria.resetFeatureCounter();
						}

					}

				});
			}
		});
		ssc.start();
		ssc.awaitTermination();
	}

}
