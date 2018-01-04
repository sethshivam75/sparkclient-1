package com.harman.spark;

import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

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

	public static BlockingQueue<String> list = new ArrayBlockingQueue(100);
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
						// if (timer != null)
						// timer.cancel();
						// timer = new Timer();
						// timer.schedule(new TimerTask() {
						//
						// @Override
						// public void run() {
						// new Thread(new ReadThread()).start();
						// }
						// }, 5 * 1000);
						System.out.println(list.size());
						if (list.size() < 100) {
							list.add(s);
							list.notify();
						}
					}
				});
			}
		});
		new Thread(sparkMongoInsertion).start();
		new Thread(mInsertionIntoMariaDB).start();
		ssc.start();
		ssc.awaitTermination();
	}

	static class ReadThread implements Runnable {

		@Override
		public void run() {
			// mInsertionIntoMariaDB.setValue(new Vector<>(list));
			// sparkMongoInsertion.setValue(new Vector<>(list));
			list.clear();
		}

	}

	private static StringBuffer stringBuffer = new StringBuffer();

}
