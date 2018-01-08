package com.harman.models;

import java.util.LinkedHashMap;

public class AppAnalyticsModel {
	private LinkedHashMap<String, Integer> mDeviceAnaModelList = new LinkedHashMap<String, Integer>();
	private String[] keys = { "SpeakerMode_Stereo", "SpeakerMode_Party", "SpeakerMode_Single", "AppToneToggle",
			"AppMFBMode", "AppHFPToggle", "AppEQMode" };
	private String macaddress;
	public String getMacaddress() {
		return macaddress;
	}

	public void setMacaddress(String macaddress) {
		this.macaddress = macaddress;
	}

	public LinkedHashMap<String, Integer> getmDeviceAnaModelList() {
		return mDeviceAnaModelList;
	}

	public void setmDeviceAnaModelList(LinkedHashMap<String, Integer> mDeviceAnaModelList) {
		this.mDeviceAnaModelList = mDeviceAnaModelList;
	}

	public String[] getKeys() {
		return keys;
	}

	public void setKeys(String[] keys) {
		this.keys = keys;
	}
}
