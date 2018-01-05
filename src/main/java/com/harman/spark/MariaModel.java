package com.harman.spark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.json.JSONArray;
import org.json.JSONObject;

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

	public String getDeviceInformation(String device_id) {
		Connection conn = null;
		Statement stmt = null;
		JSONArray jsonArray = new JSONArray();
		try {
			// STEP 2: Register JDBC driver
			Class.forName("org.mariadb.jdbc.Driver");
			// STEP 3: Open a connection
			System.out.println("Connecting to a selected database...");
			conn = DriverManager.getConnection("jdbc:mariadb://localhost/DEVICE_INFO_STORE", "root", "");
			System.out.println("Connected database successfully...");
			stmt = conn.createStatement();
			ResultSet rs;
			String query;
			if (device_id != null && !device_id.equalsIgnoreCase("")) {
				query = "SELECT * FROM device_registration WHERE device_id=" + device_id;
			} else
				query = "SELECT * FROM device_registration";
			rs = stmt.executeQuery(query);
			while (rs.next()) {
				JSONObject jsonObject = new JSONObject();
				jsonObject.put("device_id", rs.getString("device_id"));
				jsonObject.put("device_model", rs.getString("device_model"));
				jsonObject.put("operations_name", rs.getString("operations_name"));
				jsonObject.put("operations_params", rs.getString("operations_params"));
				jsonObject.put("fw_version", rs.getString("fw_version"));
				jsonObject.put("sw_version", rs.getString("sw_version"));
				jsonObject.put("connection", rs.getString("connection"));
				jsonArray.put(jsonObject);
			}

		} catch (SQLException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null) {
					conn.close();
				}
			} catch (SQLException se) {
			}
			try {
				if (conn != null) {
					conn.close();
				}
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
		if (jsonArray.length() == 0) {
			JSONObject jsonObject = new JSONObject();
			jsonObject.put("message", "Record are not found in tables.");
			return jsonObject.toString();
		} else
			return jsonArray.toString();
	}

	public String insertDeviceInformation(String device_id, String device_model, String operations_name,
			String operations_params, String fw_version, String sw_version, String connection) {
		Connection conn = null;
		Statement stmt = null;
		int response = 0;
		try {
			// STEP 2: Register JDBC driver
			Class.forName("org.mariadb.jdbc.Driver");
			// STEP 3: Open a connection
			System.out.println("Connecting to a selected database...");
			conn = DriverManager.getConnection("jdbc:mariadb://localhost/DEVICE_INFO_STORE", "root", "");
			System.out.println("Connected database successfully...");
			// STEP 4: Execute a query
			System.out.println("Creating table in given database...");
			stmt = conn.createStatement();

			Statement statement = conn.createStatement();
			String query = "INSERT INTO `device_info_table`(device_id,device_model,operations_name,operations_params,fw_version,sw_version,connection) VALUE ('"
					+ device_id + "','" + device_model + "','" + operations_name + "','" + operations_params + "','"
					+ fw_version + "','" + sw_version + "','" + connection + "')";
			response = statement.executeUpdate(query);

		} catch (SQLException se) {
			se.printStackTrace();
			System.out.println("SQLException " + se.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("SQLException " + e.getMessage());
		} finally {
			try {
				if (stmt != null) {
					conn.close();
				}
			} catch (SQLException se) {
				System.out.println("SQLException while closing data");
			}
			try {
				if (conn != null) {
					conn.close();
				}
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
		return String.valueOf(response);
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
			/*
			 * String query = "select * from " + DeviceAnalytics +
			 * " where harmanDevice_Id = " + "'" +
			 * mDeviceAnalyticsModel.getMacaddress() + "'"; ResultSet
			 * ifExistsResponse = stmt.executeQuery(query);
			 * ifExistsResponse.last(); if (ifExistsResponse.getRow() == 0) {
			 * try { String queryInsertNewRow = "INSERT INTO " + DeviceAnalytics
			 * + "(harmanDevice_Id," + Broadcaster + "," + Receiver + "," +
			 * CriticalTemperatureShutDown + "," + PowerOnOffCount + "," +
			 * EQSettings_Indoor + "," + EQSettings_Outdoor + "," +
			 * PowerBankUsage + ") VALUE ('" +
			 * mDeviceAnalyticsModel.getMacaddress() + "','" +
			 * mDeviceAnalyticsModel.getBroadcaster() + "','" +
			 * mDeviceAnalyticsModel.getReceiver() + "','" +
			 * mDeviceAnalyticsModel.getCriticalTemperatureShutDown() + "','" +
			 * mDeviceAnalyticsModel.getPowerOnOffCount() + "','" +
			 * mDeviceAnalyticsModel.getEQSettings_Indoor() + "','" +
			 * mDeviceAnalyticsModel.getEQSettings_Outdoor() + "','" +
			 * mDeviceAnalyticsModel.getPowerBankUsage() + "')"; int result =
			 * stmt.executeUpdate(queryInsertNewRow); if (result == 0) response
			 * = ErrorType.ERROR_INSERTING_DB; } catch (SQLException se) {
			 * response = ErrorType.ERROR_INSERTING_DB; } } else { try { int
			 * broadcastercount = ifExistsResponse.getInt(Broadcaster) +
			 * mDeviceAnalyticsModel.getBroadcaster(); int Receivercount =
			 * ifExistsResponse.getInt(Receiver) +
			 * mDeviceAnalyticsModel.getReceiver(); int
			 * CriticalTemperatureShutDowncount =
			 * ifExistsResponse.getInt(CriticalTemperatureShutDown) +
			 * mDeviceAnalyticsModel.getCriticalTemperatureShutDown(); int
			 * PowerOnOffCountcount = ifExistsResponse.getInt(PowerOnOffCount) +
			 * mDeviceAnalyticsModel.getPowerOnOffCount(); int
			 * EQSettings_Indoorcount =
			 * ifExistsResponse.getInt(EQSettings_Indoor) +
			 * mDeviceAnalyticsModel.getEQSettings_Indoor(); int
			 * EQSettings_Outdoorcount =
			 * ifExistsResponse.getInt(EQSettings_Outdoor) +
			 * mDeviceAnalyticsModel.getEQSettings_Outdoor(); int
			 * PowerBankUsagecount = ifExistsResponse.getInt(PowerBankUsage) +
			 * mDeviceAnalyticsModel.getPowerBankUsage();
			 * 
			 * String queryUpdate = "update " + DeviceAnalytics + " set " +
			 * Broadcaster + "=" + broadcastercount + "," + Receiver + "=" +
			 * Receivercount + "," + CriticalTemperatureShutDown + "=" +
			 * CriticalTemperatureShutDowncount + "," + PowerOnOffCount + "=" +
			 * PowerOnOffCountcount + "," + EQSettings_Indoor + "=" +
			 * EQSettings_Indoorcount + "," + EQSettings_Outdoor + "=" +
			 * EQSettings_Outdoorcount + "," + PowerBankUsage + "=" +
			 * PowerBankUsagecount + " where harmanDevice_Id = '" +
			 * mDeviceAnalyticsModel.getMacaddress() + "'"; int result =
			 * stmt.executeUpdate(queryUpdate); if (result == 0) response =
			 * ErrorType.ERROR_UPDATING_DB; } catch (SQLException se) { response
			 * = ErrorType.ERROR_UPDATING_DB; } }
			 */
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

			/*
			 * String query = "select * from " + AppAnalytics +
			 * " where harmanDevice_Id = " + "'" +
			 * mAppAnalyticsModel.getMacaddress() + "'"; ResultSet
			 * ifExistsResponse = stmt.executeQuery(query);
			 * ifExistsResponse.last(); if (ifExistsResponse.getRow() == 0) {
			 * try { String queryInsertNewRow = "INSERT INTO " + AppAnalytics +
			 * "(harmanDevice_Id," + SpeakerMode_Stereo + "," +
			 * SpeakerMode_Party + "," + SpeakerMode_Single + "," +
			 * AppSettings_AppToneToggle_On + "," +
			 * AppSettings_AppToneToggle_Off + "," +
			 * AppSettings_AppMFBMode_VoiceAssist + "," +
			 * AppSettings_AppMFBMode_PlayPause + "," +
			 * AppSettings_AppHFPToggle_On + "," + AppSettings_AppHFPToggle_Off
			 * + "," + AppSettings_AppEQMode_Indoor + "," +
			 * AppSettings_AppEQMode_Outdoor + "," +
			 * AppSettings_AppDevMode_Indoor + "," +
			 * AppSettings_AppDevMode_Outdoor + "," +
			 * AppSettings_OTAStatus_Success + "," +
			 * AppSettings_OTAStatus_Failure + "," +
			 * AppSettings_OTAStatus_Duration + ") VALUE ('" +
			 * mAppAnalyticsModel.getMacaddress() + "'," +
			 * mAppAnalyticsModel.getSpeakerMode_Stereo() + "," +
			 * mAppAnalyticsModel.getSpeakerMode_Party() + "," +
			 * mAppAnalyticsModel.getSpeakerMode_Single() + "," +
			 * mAppAnalyticsModel.getAppSettings_AppToneToggle_On() + "," +
			 * mAppAnalyticsModel.getAppSettings_AppToneToggle_Off() + "," +
			 * mAppAnalyticsModel.getAppSettings_AppMFBMode_VoiceAssist() + ","
			 * + mAppAnalyticsModel.getAppSettings_AppMFBMode_PlayPause() + ","
			 * + mAppAnalyticsModel.getAppSettings_AppHFPToggle_On() + "," +
			 * mAppAnalyticsModel.getAppSettings_AppHFPToggle_Off() + "," +
			 * mAppAnalyticsModel.getAppSettings_AppEQMode_Indoor() + "," +
			 * mAppAnalyticsModel.getAppSettings_AppEQMode_Outdoor() + "," +
			 * mAppAnalyticsModel.getAppSettings_AppDevMode_Indoor() + "," +
			 * mAppAnalyticsModel.getAppSettings_AppDevMode_Outdoor() + "," +
			 * mAppAnalyticsModel.getAppSettings_OTAStatus_Success() + "," +
			 * mAppAnalyticsModel.getAppSettings_OTAStatus_Failure() + "," +
			 * mAppAnalyticsModel.getAppSettings_OTAStatus_Duration() + ")"; int
			 * result = stmt.executeUpdate(queryInsertNewRow); if (result == 0)
			 * response = ErrorType.ERROR_INSERTING_DB; } catch (SQLException
			 * se) { response = ErrorType.ERROR_INSERTING_DB; } } else { try {
			 * int AppSettings_AppToneToggle_OnCount =
			 * ifExistsResponse.getInt(AppSettings_AppToneToggle_On) +
			 * mAppAnalyticsModel.getAppSettings_AppToneToggle_On(); int
			 * AppSettings_AppToneToggle_OffCount =
			 * ifExistsResponse.getInt(AppSettings_AppToneToggle_Off) +
			 * mAppAnalyticsModel.getAppSettings_AppToneToggle_Off();
			 * 
			 * int SpeakerMode_StereoCount =
			 * ifExistsResponse.getInt(SpeakerMode_Stereo) +
			 * mAppAnalyticsModel.getSpeakerMode_Stereo(); int
			 * SpeakerMode_PartyCount =
			 * ifExistsResponse.getInt(SpeakerMode_Party) +
			 * mAppAnalyticsModel.getSpeakerMode_Party(); int
			 * SpeakerMode_SingleCount =
			 * ifExistsResponse.getInt(SpeakerMode_Single) +
			 * mAppAnalyticsModel.getSpeakerMode_Single();
			 * 
			 * int AppSettings_AppMFBMode_VoiceAssistCount = ifExistsResponse
			 * .getInt(AppSettings_AppMFBMode_VoiceAssist) +
			 * mAppAnalyticsModel.getAppSettings_AppMFBMode_VoiceAssist(); int
			 * AppSettings_AppMFBMode_PlayPauseCount = ifExistsResponse
			 * .getInt(AppSettings_AppMFBMode_PlayPause) +
			 * mAppAnalyticsModel.getAppSettings_AppMFBMode_PlayPause();
			 * 
			 * int AppSettings_AppHFPToggle_OnCount =
			 * ifExistsResponse.getInt(AppSettings_AppHFPToggle_On) +
			 * mAppAnalyticsModel.getAppSettings_AppHFPToggle_On(); int
			 * AppSettings_AppHFPToggle_OffCount =
			 * ifExistsResponse.getInt(AppSettings_AppHFPToggle_Off) +
			 * mAppAnalyticsModel.getAppSettings_AppHFPToggle_Off();
			 * 
			 * int AppSettings_AppEQMode_IndoorCount =
			 * ifExistsResponse.getInt(AppSettings_AppEQMode_Indoor) +
			 * mAppAnalyticsModel.getAppSettings_AppEQMode_Indoor(); int
			 * AppSettings_AppEQMode_OutdoorCount =
			 * ifExistsResponse.getInt(AppSettings_AppEQMode_Outdoor) +
			 * mAppAnalyticsModel.getAppSettings_AppEQMode_Outdoor();
			 * 
			 * int AppSettings_AppDevMode_IndoorCount =
			 * ifExistsResponse.getInt(AppSettings_AppDevMode_Indoor) +
			 * mAppAnalyticsModel.getAppSettings_AppDevMode_Indoor(); int
			 * AppSettings_AppDevMode_OutdoorCount =
			 * ifExistsResponse.getInt(AppSettings_AppDevMode_Outdoor) +
			 * mAppAnalyticsModel.getAppSettings_AppDevMode_Outdoor();
			 * 
			 * int AppSettings_OTAStatus_SuccessCount =
			 * ifExistsResponse.getInt(AppSettings_OTAStatus_Success) +
			 * mAppAnalyticsModel.getAppSettings_OTAStatus_Success(); int
			 * AppSettings_OTAStatus_FailureCount =
			 * ifExistsResponse.getInt(AppSettings_OTAStatus_Failure) +
			 * mAppAnalyticsModel.getAppSettings_OTAStatus_Failure(); int
			 * AppSettings_OTAStatus_DurationCount =
			 * ifExistsResponse.getInt(AppSettings_OTAStatus_Duration) +
			 * mAppAnalyticsModel.getAppSettings_OTAStatus_Duration();
			 * 
			 * String queryUpdate = "update " + AppAnalytics + " set " +
			 * AppSettings_AppToneToggle_On + "= " +
			 * AppSettings_AppToneToggle_OnCount + "," +
			 * AppSettings_AppToneToggle_Off + "= " +
			 * AppSettings_AppToneToggle_OffCount + "," + SpeakerMode_Stereo +
			 * " = " + SpeakerMode_StereoCount + "," + SpeakerMode_Single +
			 * " = " + SpeakerMode_SingleCount + "," + SpeakerMode_Party + "= "
			 * + SpeakerMode_PartyCount + "," +
			 * AppSettings_AppMFBMode_VoiceAssist + "= " +
			 * AppSettings_AppMFBMode_VoiceAssistCount + "," +
			 * AppSettings_AppMFBMode_PlayPause + "= " +
			 * AppSettings_AppMFBMode_PlayPauseCount + "," +
			 * AppSettings_AppHFPToggle_On + "= " +
			 * AppSettings_AppHFPToggle_OnCount + "," +
			 * AppSettings_AppHFPToggle_Off + "= " +
			 * AppSettings_AppHFPToggle_OffCount + "," +
			 * AppSettings_AppEQMode_Indoor + "= " +
			 * AppSettings_AppEQMode_IndoorCount + "," +
			 * AppSettings_AppEQMode_Outdoor + "= " +
			 * AppSettings_AppEQMode_OutdoorCount + "," +
			 * AppSettings_AppDevMode_Indoor + "= " +
			 * AppSettings_AppDevMode_IndoorCount + "," +
			 * AppSettings_AppDevMode_Outdoor + "= " +
			 * AppSettings_AppDevMode_OutdoorCount + "," +
			 * AppSettings_OTAStatus_Success + "= " +
			 * AppSettings_OTAStatus_SuccessCount + "," +
			 * AppSettings_OTAStatus_Failure + "= " +
			 * AppSettings_OTAStatus_FailureCount + "," +
			 * AppSettings_OTAStatus_Duration + "= " +
			 * AppSettings_OTAStatus_DurationCount +
			 * " where harmanDevice_Id = '" + mAppAnalyticsModel.getMacaddress()
			 * + "'"; int result = stmt.executeUpdate(queryUpdate); if (result
			 * == 0) response = ErrorType.ERROR_UPDATING_DB; } catch
			 * (SQLException se) { response = ErrorType.ERROR_UPDATING_DB; } }
			 */
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
