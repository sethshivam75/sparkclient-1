package com.harman.models;

import java.util.LinkedHashMap;

public class DeviceAnalyticsModel {

	private LinkedHashMap<String, Integer> mDeviceAnaModelList = new LinkedHashMap<String, Integer>();
	private String[] keys = { "Broadcaster", "Receiver", "CriticalTemperatureShutDown", "PowerOnOffCount", "EQSettings",
			"PowerBankUsage" };
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

}
