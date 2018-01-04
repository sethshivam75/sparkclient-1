package com.harman.spark;

import java.sql.Connection;

import com.harman.utils.ErrorType;

public interface MariaStructure {

	public String getDeviceInformation(String macID);

	public String insertDeviceInformation(String device_id, String device_model, String operations_name,
			String operations_params, String fw_version, String sw_version, String connection);

	public ErrorType insertDeviceModel(HarmanDeviceModel mHarmanDeviceModel, Connection conn);

	public ErrorType insertDeviceAnalytics(DeviceAnalyticsModel mDeviceAnalyticsModel, Connection conn);

	public ErrorType insertAppAnalytics(AppAnalyticsModel mAppAnalyticsModel, Connection conn);
}
