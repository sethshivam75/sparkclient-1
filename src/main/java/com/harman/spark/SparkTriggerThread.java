package com.harman.spark;

import java.io.Serializable;

public class SparkTriggerThread implements Runnable, Serializable {

	private String feature;

	private SparkTriggerThread(String feature) {
		this.feature = feature;
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public static void SendEmail(String feature, int value) {

		new Thread(new SparkTriggerThread(feature)).start();

	}

	@Override
	public void run() {
		System.out.println("****ALERT " + feature + " is need to be investigated !!!!!!!!!!!!!!!!!!");
	}

}
