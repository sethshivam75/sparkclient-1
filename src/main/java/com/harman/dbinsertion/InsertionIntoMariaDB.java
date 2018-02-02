package com.harman.dbinsertion;

import java.sql.Connection;

import org.json.JSONException;
import org.json.JSONObject;

import com.harman.models.AppAnalyticsModel;
import com.harman.models.DBkeys;
import com.harman.models.DeviceAnalyticsModel;
import com.harman.models.HarmanDeviceModel;
import com.harman.utils.ErrorType;
import com.harman.utils.HarmanParser;

public class InsertionIntoMariaDB implements DBkeys {

	private InsertionIntoMariaDB() {

	}

	static InsertionIntoMariaDB isInsertionIntoMariaDB = null;

	public static InsertionIntoMariaDB getInstance() {
		if (isInsertionIntoMariaDB == null)
			isInsertionIntoMariaDB = new InsertionIntoMariaDB();
		return isInsertionIntoMariaDB;
	}

	private int featureCounter = 0;

	public String insertIntoMariaDB(String record) {
		System.out.println("****************************** Inserting to mariaDB");
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
				//System.out.println(errorType.name());
			} catch (JSONException e) {
				errorType = ErrorType.INVALID_JSON;
			}

			try {
				DeviceAnalyticsModel deviceAnalyticsModel = harmanParser.getParseDeviceAnalyticsModel(
						jsonObject.getJSONObject(DeviceAnalytics), deviceModel.getMacAddress());
				errorType = mariaModel.insertDeviceAnalytics(deviceAnalyticsModel, connection);
				
				updateFeatureCounter(deviceAnalyticsModel.getmDeviceAnaModelList().get(CriticalTemperatureShutDown));
				
				//System.out.println(errorType.name());
			} catch (JSONException e) {
				errorType = ErrorType.INVALID_JSON;
			}

			try {
				AppAnalyticsModel appAnalyticsModel = harmanParser
						.getParseAppAnalyticsModel(jsonObject.getJSONObject(AppAnalytics), deviceModel.getMacAddress());
				errorType = mariaModel.insertAppAnalytics(appAnalyticsModel, connection);
				//System.out.println(errorType.name());
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

	public int getFeatureCounter() {
		return featureCounter;
	}

	public void updateFeatureCounter(int featureCounter) {
		featureCounter += featureCounter;
	}

	public void resetFeatureCounter() {
		featureCounter = 0;
	}

}