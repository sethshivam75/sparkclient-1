package com.harman.dbinsertion;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.harman.models.AppAnalyticsModel;
import com.harman.models.DBkeys;
import com.harman.models.DeviceAnalyticsModel;
import com.harman.models.HarmanDeviceModel;
import com.harman.models.MariaStructure;
import com.harman.utils.ErrorType;

public class MariaModel implements MariaStructure, DBkeys {

	static MariaModel mariaModel;

	public static MariaModel getInstance() {
		if (mariaModel == null)
			mariaModel = new MariaModel();
		return mariaModel;
	}

	Connection connn = null;

	public Connection openConnection() {
		if (connn == null)
			return connn;
		try {
			Class.forName("org.mariadb.jdbc.Driver");
			// STEP 3: Open a connection
			System.out.println("Connecting to a selected database...");
			connn = DriverManager.getConnection("jdbc:mariadb://localhost/DEVICE_INFO_STORE", "root", "");
			// connn =
			// DriverManager.getConnection("jdbc:mariadb://127.0.0.1/device_info_store",
			// "root", "abcd123");
			System.out.println("Connected database successfully...");
		} catch (SQLException e) {
			System.out.println("Failed to connect db");
		} catch (Exception e) {
			System.out.println("Failed to connect db");
		}
		return connn;
	}

	public void closeConnection() {
		try {
			if (connn != null) {
				connn.close();
			}
		} catch (SQLException se) {
			se.printStackTrace();
		}
	}

	public ErrorType insertDeviceModel(HarmanDeviceModel mHarmanDeviceModel, Connection conn) {
		ErrorType response = ErrorType.NO_ERROR;
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			String query = "select * from " + harmanDevice + " where " + macAddress + " = " + "'"
					+ mHarmanDeviceModel.getMacAddress() + "'";
			ResultSet ifExistsResponse = stmt.executeQuery(query);
			ifExistsResponse.last();
			if (ifExistsResponse.getRow() == 0) {
				try {
					String queryInsertNewRow = "INSERT INTO " + harmanDevice + "(" + macAddress + "," + productId + ","
							+ colorId + "," + productName + "," + colorName + "," + FirmwareVersion + "," + AppVersion
							+ ") VALUE ('" + mHarmanDeviceModel.getMacAddress() + "','"
							+ mHarmanDeviceModel.getProductId() + "','" + mHarmanDeviceModel.getColorId() + "','"
							+ mHarmanDeviceModel.getProductName() + "','" + mHarmanDeviceModel.getColorName() + "','"
							+ mHarmanDeviceModel.getFirmwareVersion() + "','" + mHarmanDeviceModel.getAppVersion()
							+ "')";
					int result = stmt.executeUpdate(queryInsertNewRow);
					if (result == 0)
						response = ErrorType.ERROR_INSERTING_DB;
				} catch (SQLException se) {
					response = ErrorType.ERROR_INSERTING_DB;
				}
			} else {
				try {
					String queryUpdate = "update " + harmanDevice + " set " + FirmwareVersion + "= '"
							+ mHarmanDeviceModel.getFirmwareVersion() + "'," + AppVersion + " = '"
							+ mHarmanDeviceModel.getAppVersion() + "' where " + macAddress + " = '"
							+ mHarmanDeviceModel.getMacAddress() + "'";
					int result = stmt.executeUpdate(queryUpdate);
					if (result == 0)
						response = ErrorType.ERROR_UPDATING_DB;
				} catch (SQLException se) {
					response = ErrorType.ERROR_UPDATING_DB;
				}
			}
		} catch (Exception e) {
			response = ErrorType.NETWORK_NOT_AVAILBLE;
		} finally {
			try {
				if (stmt != null) {
					stmt.close();
				}
			} catch (SQLException se) {
				response = ErrorType.ERROR_CLOSING_DB;
				System.out.println("SQLException while closing data");
			}
		}
		return response;
	}

	public ErrorType insertDeviceAnalytics(DeviceAnalyticsModel mDeviceAnalyticsModel, Connection conn) {
		ErrorType response = ErrorType.NO_ERROR;
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			try {
				String queryInsertNewRow = createQuery(mDeviceAnalyticsModel.getmDeviceAnaModelList(), DeviceAnalytics,
						mDeviceAnalyticsModel.getMacaddress()).toString();
				int result = stmt.executeUpdate(queryInsertNewRow);
				if (result == 0)
					response = ErrorType.ERROR_INSERTING_DB;
			} catch (SQLException se) {
				response = ErrorType.ERROR_INSERTING_DB;
			}
		} catch (Exception e) {
			response = ErrorType.NETWORK_NOT_AVAILBLE;
		} finally {
			try {
				if (stmt != null) {
					stmt.close();
				}
			} catch (SQLException se) {
				response = ErrorType.ERROR_CLOSING_DB;
				System.out.println("SQLException while closing data");
			}
		}
		return response;
	}

	public ErrorType insertAppAnalytics(AppAnalyticsModel mAppAnalyticsModel, Connection conn) {
		ErrorType response = ErrorType.NO_ERROR;
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			try {
				String queryInsertNewRow = createQuery(mAppAnalyticsModel.getmDeviceAnaModelList(), AppAnalytics,
						mAppAnalyticsModel.getMacaddress()).toString();
				int result = stmt.executeUpdate(queryInsertNewRow);
				if (result == 0)
					response = ErrorType.ERROR_INSERTING_DB;
			} catch (SQLException se) {
				response = ErrorType.ERROR_INSERTING_DB;
			}
		} catch (Exception e) {
			response = ErrorType.NETWORK_NOT_AVAILBLE;
		} finally {
			try {
				if (stmt != null) {
					stmt.close();
				}
			} catch (SQLException se) {
				response = ErrorType.ERROR_CLOSING_DB;
				System.out.println("SQLException while closing data");
			}
		}
		return response;
	}

	@SuppressWarnings("rawtypes")
	public StringBuffer createQuery(LinkedHashMap<String, Integer> keyValueMap, String table, String macAddress) {
		Iterator<Entry<String, Integer>> it = keyValueMap.entrySet().iterator();
		StringBuffer queryBuffer = new StringBuffer();

		StringBuffer bufferKey = new StringBuffer();
		StringBuffer bufferValue = new StringBuffer();

		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry) it.next();
			if (it.hasNext()) {
				bufferKey.append(pair.getKey() + ",");
				bufferValue.append(pair.getValue() + ",");
			} else {
				bufferKey.append(pair.getKey());
				bufferValue.append(pair.getValue());
			}
			System.out.println(pair.getKey() + " = " + pair.getValue());
			it.remove(); // avoids a ConcurrentModificationException
		}
		queryBuffer.append("INSERT INTO " + table + "(harmanDevice_Id," + bufferKey + ") VALUE ( '" + macAddress + "',"
				+ bufferValue + " )");

		System.out.println(queryBuffer);
		return queryBuffer;
	}
}
