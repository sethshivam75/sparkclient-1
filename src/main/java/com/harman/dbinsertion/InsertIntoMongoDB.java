package com.harman.dbinsertion;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Vector;

import org.bson.Document;

import com.harman.spark.SparkClient;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class InsertIntoMongoDB implements Runnable {

	Vector<StringBuffer> listofJson = new Vector<StringBuffer>();
	Object object = new Object();

	public void setValue(Vector<StringBuffer> json) {

		synchronized (object) {
			this.listofJson.addAll(json);
		}
	}

	public Vector<StringBuffer> getValues() {
		synchronized (object) {
			Vector<StringBuffer> temp = new Vector<>(listofJson);
			listofJson.clear();
			return temp;
		}
	}

	private void inserIntoMongoDB(Vector<StringBuffer> json) {
		System.out.println(json);
		List<Document> list = new ArrayList<>();
		for (StringBuffer temp : json) {
			Document document = Document.parse(temp.toString());
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
				inserIntoMongoDB(new Vector<StringBuffer>(getValues()));
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