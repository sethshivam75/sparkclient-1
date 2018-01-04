package com.harman.utils;

import org.json.JSONException;
import org.json.JSONObject;

import com.harman.spark.AppAnalyticsModel;
import com.harman.spark.DeviceAnalyticsModel;
import com.harman.spark.HarmanDeviceModel;

public class HarmanParser {

	public HarmanDeviceModel getParseHarmanDevice(JSONObject mHarmanDevice) throws JSONException {

		HarmanDeviceModel harmanModel = new HarmanDeviceModel();
		harmanModel.setAppVersion(mHarmanDevice.getString("AppVersion"));
		harmanModel.setFirmwareVersion(mHarmanDevice.getString("FirmwareVersion"));
		harmanModel.setColorName(mHarmanDevice.getString("colorName"));
		harmanModel.setProductName(mHarmanDevice.getString("productName"));
		harmanModel.setColorId(mHarmanDevice.getInt("colorId"));
		harmanModel.setProductId(mHarmanDevice.getInt("productId"));
		harmanModel.setMacAddress(mHarmanDevice.getString("macAddress"));

		return harmanModel;
	}

	public DeviceAnalyticsModel getParseDeviceAnalyticsModel(JSONObject mAanaytics, String macaddress)
			throws JSONException {

		DeviceAnalyticsModel deviceAnalyticsModel = new DeviceAnalyticsModel();
		String[] keys = deviceAnalyticsModel.getKeys();
		for (String key : keys) {
			if (!mAanaytics.isNull(key)) {
				deviceAnalyticsModel.getmDeviceAnaModelList().put(key, mAanaytics.getInt(key));
			}
		}
		deviceAnalyticsModel.setMacaddress(macaddress);
		/*
		 * deviceAnalyticsModel.setBroadcaster(mAanaytics.getInt("Broadcaster"))
		 * ; deviceAnalyticsModel.setReceiver(mAanaytics.getInt("Receiver"));
		 * deviceAnalyticsModel.setCriticalTemperatureShutDown(mAanaytics.getInt
		 * ("CriticalTemperatureShutDown"));
		 * deviceAnalyticsModel.setPowerBankUsage(mAanaytics.getInt(
		 * "PowerBankUsage"));
		 * deviceAnalyticsModel.setPowerOnOffCount(mAanaytics.getInt(
		 * "PowerOnOffCount"));
		 * 
		 * JSONObject mEQSettings = mAanaytics.getJSONObject("EQSettings");
		 * deviceAnalyticsModel.setEQSettings_Indoor(mEQSettings.getInt("Indoor"
		 * )); deviceAnalyticsModel.setEQSettings_Outdoor(mEQSettings.getInt(
		 * "Outdoor")); deviceAnalyticsModel.setMacaddress(macaddress);
		 */
		return deviceAnalyticsModel;

	}

	public AppAnalyticsModel getParseAppAnalyticsModel(JSONObject mAppAnalytics, String macaddress)
			throws JSONException {

		AppAnalyticsModel analyticsModel = new AppAnalyticsModel();

		String[] keys = analyticsModel.getKeys();
		for (String key : keys) {
			if (!mAppAnalytics.isNull(key))
				analyticsModel.getmDeviceAnaModelList().put(key, mAppAnalytics.getInt(key));
		}

		if (!mAppAnalytics.isNull("SpeakerMode")) {

			JSONObject jsonObject = mAppAnalytics.getJSONObject("SpeakerMode");

			if (!jsonObject.isNull("Stereo")) {
				analyticsModel.getmDeviceAnaModelList().put("SpeakerMode_Stereo", jsonObject.getInt("Stereo"));
			}
			if (!jsonObject.isNull("Party")) {
				analyticsModel.getmDeviceAnaModelList().put("SpeakerMode_Party", jsonObject.getInt("Party"));
			}
			if (!jsonObject.isNull("Single")) {
				analyticsModel.getmDeviceAnaModelList().put("SpeakerMode_Single", jsonObject.getInt("Single"));
			}

		}
		analyticsModel.setMacaddress(macaddress);
		/*
		 * JSONObject jSpeakerMode = mAppAnalytics.getJSONObject("SpeakerMode");
		 * analyticsModel.setSpeakerMode_Stereo(jSpeakerMode.getInt("Stereo"));
		 * analyticsModel.setSpeakerMode_Party(jSpeakerMode.getInt("Party"));
		 * analyticsModel.setSpeakerMode_Single(jSpeakerMode.getInt("Single"));
		 * 
		 * JSONObject jAppSettings = mAppAnalytics.getJSONObject("AppSettings");
		 * 
		 * JSONObject jAppToneToggle =
		 * jAppSettings.getJSONObject("AppToneToggle");
		 * analyticsModel.setAppSettings_AppToneToggle_On(jAppToneToggle.getInt(
		 * "On"));
		 * analyticsModel.setAppSettings_AppToneToggle_Off(jAppToneToggle.getInt
		 * ("Off"));
		 * 
		 * JSONObject jAppMFBMode = jAppSettings.getJSONObject("AppMFBMode");
		 * analyticsModel.setAppSettings_AppMFBMode_VoiceAssist(jAppMFBMode.
		 * getInt("VoiceAssist"));
		 * analyticsModel.setAppSettings_AppMFBMode_PlayPause(jAppMFBMode.getInt
		 * ("PlayPause"));
		 * 
		 * JSONObject jAppHFPToggle =
		 * jAppSettings.getJSONObject("AppHFPToggle");
		 * analyticsModel.setAppSettings_AppHFPToggle_On(jAppHFPToggle.getInt(
		 * "On"));
		 * analyticsModel.setAppSettings_AppHFPToggle_Off(jAppHFPToggle.getInt(
		 * "Off"));
		 * 
		 * JSONObject jAppEQMode = jAppSettings.getJSONObject("AppEQMode");
		 * analyticsModel.setAppSettings_AppEQMode_Indoor(jAppEQMode.getInt(
		 * "Indoor"));
		 * analyticsModel.setAppSettings_AppEQMode_Outdoor(jAppEQMode.getInt(
		 * "Outdoor"));
		 * 
		 * JSONObject jAppDevMode = jAppSettings.getJSONObject("AppDevMode");
		 * analyticsModel.setAppSettings_AppDevMode_Indoor(jAppDevMode.getInt(
		 * "Indoor"));
		 * analyticsModel.setAppSettings_AppDevMode_Outdoor(jAppDevMode.getInt(
		 * "Outdoor"));
		 * 
		 * JSONObject jOTAStatus = mAppAnalytics.getJSONObject("OTAStatus");
		 * analyticsModel.setAppSettings_OTAStatus_Success(jOTAStatus.getInt(
		 * "Success"));
		 * analyticsModel.setAppSettings_OTAStatus_Failure(jOTAStatus.getInt(
		 * "Failure"));
		 * analyticsModel.setAppSettings_OTAStatus_Duration(jOTAStatus.getInt(
		 * "Duration"));
		 */

		analyticsModel.setMacaddress(macaddress);

		return analyticsModel;

	}
}
