package com.harman.dbinsertion;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class InsertIntoMongoDB {

	Vector<String> listofJson = new Vector<String>();
	Object object = new Object();
	static InsertIntoMongoDB insertIntoMongoDB = null;

	private InsertIntoMongoDB() {

	}

	private long counter;

	public static InsertIntoMongoDB getInstance() {
		if (insertIntoMongoDB == null)
			insertIntoMongoDB = new InsertIntoMongoDB();
		return insertIntoMongoDB;
	}

	MongoClient mongoClient = null;

	/*
	 * Connection opens once only, and kept open till the Sparkclient runs
	 */
	public void openConnection() {
		if (mongoClient == null)
			mongoClient = new MongoClient("localhost", 27017);
	}

	public void inserSingleRecordMongoDB(String json) {
		try {
			System.out.println("*************Inserting into mongo");
			Document document = Document.parse(json.toString());
			MongoDatabase database = mongoClient.getDatabase("DEVICE_INFO_STORE");
			MongoCollection<Document> table = database.getCollection("SmartAudioAnalytics");
			table.insertOne(document);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void inserIntoMongoDB(Vector<String> json) {
		//System.out.println(json);
		List<Document> list = new ArrayList<>();
		for (String temp : json) {
			Document document = Document.parse(temp.toString());
			list.add(document);
		}
		MongoClient mongoClient = new MongoClient("localhost", 27017);
		MongoDatabase database = mongoClient.getDatabase("DEVICE_INFO_STORE");
		MongoCollection<Document> table = database.getCollection("SmartAudioAnalytics");
		table.insertMany(list);
		mongoClient.close();
	}

	public long getCounter() {
		return counter;
	}

	public void updateCounter() {
		++counter;
	}

}