package com.harman.models;

import java.sql.Connection;

import com.harman.utils.ErrorType;

public interface MariaStructure {


	public ErrorType insertDeviceModel(HarmanDeviceModel mHarmanDeviceModel, Connection conn);

	public ErrorType insertDeviceAnalytics(DeviceAnalyticsModel mDeviceAnalyticsModel, Connection conn);

	public ErrorType insertAppAnalytics(AppAnalyticsModel mAppAnalyticsModel, Connection conn);
}
