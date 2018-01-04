package com.harman.spark;

import java.util.ConcurrentModificationException;
import java.util.Timer;
import java.util.TimerTask;
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

public class SparkClient implements DBkeys {

	public static Vector<StringBuffer> list = new Vector<>();
	static Timer timer;
	@SuppressWarnings("unused")
	private JavaStreamingContext ssc = null;
	static InsertIntoMongoDB sparkMongoInsertion;
	static InsertionIntoMariaDB mInsertionIntoMariaDB;

	@SuppressWarnings("resource")
	public static void main(String[] args) {
		System.out.println("In main spark Client");
		mInsertionIntoMariaDB = new InsertionIntoMariaDB();
		sparkMongoInsertion = new InsertIntoMongoDB();
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

				System.out.println("javaRDD");
				rdd.foreach(new VoidFunction<String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public void call(String s) throws Exception {
						try {
							list.add(stringBuffer);
						} catch (ConcurrentModificationException e) {
							System.out.println("value is missed");
						}
					}
				});
			}
		});

		timer = new Timer();
		timer.scheduleAtFixedRate(new TimerTask() {

			@Override
			public void run() {
				new Thread(new ReadThread()).start();
			}
		}, 35 * 1000, 7000);
		new Thread(sparkMongoInsertion).start();
		new Thread(mInsertionIntoMariaDB).start();
		ssc.start();
		ssc.awaitTermination();
	}

	static class ReadThread implements Runnable {

		@Override
		public void run() {
			try {
				if (list.size() > 0) {
					mInsertionIntoMariaDB.setValue(new Vector<>(list));
					sparkMongoInsertion.setValue(new Vector<>(list));
					list.clear();
				}
			} catch (ConcurrentModificationException e) {

			}
		}

	}

	private static StringBuffer stringBuffer = new StringBuffer();

}
