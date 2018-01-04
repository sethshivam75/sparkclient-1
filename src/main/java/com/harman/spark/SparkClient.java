package com.harman.spark;

import java.util.ConcurrentModificationException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;
import java.util.logging.Handler;

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

	@SuppressWarnings("resource")
	public static void main(String[] args) {
		InsertionIntoMariaDB mInsertionIntoMariaDB = new InsertionIntoMariaDB();
		InsertIntoMongoDB sparkMongoInsertion;
		System.out.println("In main spark Client");
		SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("SmartAudioAnalytics");
		JavaSparkContext context = new JavaSparkContext(sparkConf);
		JavaStreamingContext ssc = new JavaStreamingContext(context, new Duration(30000));
		// TODO close ssc connection.
		sparkMongoInsertion = new InsertIntoMongoDB();
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
						if (timer != null)
							timer.cancel();
						timer = new Timer();
						timer.schedule(new TimerTask() {

							@Override
							public void run() {
								mInsertionIntoMariaDB.setValue(list);
								sparkMongoInsertion.setValue(list);
								list.clear();

							}
						}, 5 * 1000);
						if (s.trim().equals(";") || s.trim().equals(";\n")) {
							list.add(stringBuffer);
							System.out.println("*****************[TA] outPut =" + stringBuffer);
							stringBuffer.setLength(0);
						} else {
							stringBuffer.append(s);
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

	private static StringBuffer stringBuffer = new StringBuffer();

}
