package com.harman.dbinsertion;

import java.sql.Connection;
import java.util.Vector;

import org.json.JSONException;
import org.json.JSONObject;

import com.harman.spark.AppAnalyticsModel;
import com.harman.spark.DBkeys;
import com.harman.spark.DeviceAnalyticsModel;
import com.harman.spark.HarmanDeviceModel;
import com.harman.spark.MariaModel;
import com.harman.utils.ErrorType;
import com.harman.utils.HarmanParser;

public class InsertionIntoMariaDB implements Runnable, DBkeys {

	Vector<String> listofJson = new Vector<String>();
	Object object = new Object();

	public void setValue(Vector<String> json) {

		synchronized (object) {
			this.listofJson.addAll(json);
		}
	}

	public Vector<String> getValues() {
		synchronized (object) {
			Vector<String> temp = new Vector<>(listofJson);
			listofJson.clear();
			return temp;
		}
	}

	@Override
	public void run() {

		while (true) {
			Vector<String> listofJson = getValues();
			for (String record : listofJson) {
				System.out.println("Inserting into mariadb");
				String response = insertIntoMariaDB(record.toString());
				System.out.println(response);
			}
			try {
				System.out.println("MariaDB thread in sleep");
				Thread.sleep(5000);
			} catch (Exception e) {
				System.out.println("MariaDB thread throws exception during sleep");
			}
		}
	}

	private InsertionIntoMariaDB() {

	}

	static InsertionIntoMariaDB isInsertionIntoMariaDB = null;

	public static InsertionIntoMariaDB getInstance() {
		if (isInsertionIntoMariaDB == null)
			isInsertionIntoMariaDB = new InsertionIntoMariaDB();
		return isInsertionIntoMariaDB;
	}

	public String insertIntoMariaDB(String record) {
		ErrorType errorType = ErrorType.NO_ERROR;
		JSONObject response = new JSONObject();
		try {
			JSONObject jsonObject = new JSONObject(record);
			MariaModel mariaModel = MariaModel.getInstance();
			Connection connection = mariaModel.openConnection();
			HarmanParser harmanParser = new HarmanParser();
			HarmanDeviceModel deviceModel = null;
			try {
				deviceModel = harmanParser.getParseHarmanDevice(jsonObject.getJSONObject(harmanDevice));
				errorType = mariaModel.insertDeviceModel(deviceModel, connection);
				System.out.println(errorType.name());
			} catch (JSONException e) {
				errorType = ErrorType.INVALID_JSON;
			}

			try {
				DeviceAnalyticsModel deviceAnalyticsModel = harmanParser.getParseDeviceAnalyticsModel(
						jsonObject.getJSONObject(DeviceAnalytics), deviceModel.getMacAddress());
				errorType = mariaModel.insertDeviceAnalytics(deviceAnalyticsModel, connection);
				System.out.println(errorType.name());
			} catch (JSONException e) {
				errorType = ErrorType.INVALID_JSON;
			}

			try {
				AppAnalyticsModel appAnalyticsModel = harmanParser
						.getParseAppAnalyticsModel(jsonObject.getJSONObject(AppAnalytics), deviceModel.getMacAddress());
				errorType = mariaModel.insertAppAnalytics(appAnalyticsModel, connection);
				System.out.println(errorType.name());
			} catch (JSONException e) {
				errorType = ErrorType.INVALID_JSON;
			}

			switch (errorType) {
			case NO_ERROR:
				response.put("Status", 1);
				break;

			default:
				response.put("Status", 0);
				break;
			}
			response.put("cmd", "UpdateSmartAudioAnalyticsRes");
		} catch (Exception e) {
			response.put("Status", 0);
			response.put("cmd", "UpdateSmartAudioAnalyticsRes");
			System.out.println("fail to parse");
		} finally {
			/*
			 * MariaModel mariaModel = MariaModel.getInstance();
			 * mariaModel.closeConnection();
			 */
		}
		System.out.println(errorType.name());
		return response.toString();
	}

}