package com.harman.spark;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class SparkClient {
	static Vector<String> list = new Vector<>();
	JavaStreamingContext ssc = null;
	static String TAG = "*****************[TA]";

	@SuppressWarnings("resource")
	public static void main(String[] args) {

		// Consumer consumer = new SparkClient.Consumer(queue);
		SparkMongoInsertion sparkMongoInsertion;
		System.out.println("In main spark Client");
		SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("SmartAudioAnalytics");
		JavaSparkContext context = new JavaSparkContext(sparkConf);
		JavaStreamingContext ssc = new JavaStreamingContext(context, new Duration(60000));
		// TODO close ssc connection.
		sparkMongoInsertion = new SparkClient.SparkMongoInsertion();

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
						
						System.out.println("*****************[TA] size =" + list.size());
						System.out.println("*****************[TA] outPut =" + stringBuffer);
						try {
							if (s.trim().equals(";")||s.trim().equals(";\n")) {
								list.add(stringBuffer.toString());
								System.out.println("*****************[TA] outPut =" + stringBuffer);
								stringBuffer.setLength(0);
							} else {
								stringBuffer.append(s);
							}
						} catch (ConcurrentModificationException e) {
							System.out.println(SparkClient.TAG + " ConcurrentModificationException(javaRDD)");
						}
					}
				});
			}
		});
		new Thread(sparkMongoInsertion).start();
		ssc.start();
		ssc.awaitTermination();
	}

	static StringBuffer stringBuffer = new StringBuffer();

	static public class Consumer implements Runnable {

		protected BlockingQueue<String> queue = null;

		public Consumer(BlockingQueue<String> queue) {
			this.queue = queue;
		}

		public void run() {
			try {
				System.out.println(queue.take());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public static class SparkMongoInsertion implements Runnable {

		public SparkMongoInsertion() {

		}

		public void inserIntoMongoDB(Vector<String> json) {
			System.out.println(json);
			List<Document> list = new ArrayList<>();
			for (String temp : json) {
				Document document = Document.parse(temp);
				list.add(document);
			}
			MongoClient mongoClient = new MongoClient("localhost", 27017);
			MongoDatabase database = mongoClient.getDatabase("DEVICE_INFO_STORE");
			MongoCollection<Document> table = database.getCollection("SmartAudioAnalytics");
			table.insertMany(list);
			mongoClient.close();
		}

		@Override
		public void run() {

			while (true) {
				try {
					if (SparkClient.list.size() > 0) {
						inserIntoMongoDB(new Vector<String>(SparkClient.list));
						SparkClient.list.clear();
					}
				} catch (ConcurrentModificationException e) {
					System.out.println(SparkClient.TAG + " ConcurrentModificationException");
				}
				try {
					System.out.println(SparkClient.TAG + "Thread sleep.");
					Thread.sleep(20 * 1000);
				} catch (Exception e) {

				}

			}
		}
	}

}
