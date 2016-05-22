package com.xAd.demo;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.DateTimeException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opencsv.CSVReader;

public class Worker implements Runnable {

	CSVReader impsReader = null;
	CSVReader clicksReader = null;
	int commonCol = 1;
	HashMap<String, String> deviceTypeMap;
	HashMap<String, String> connectionTypeMap;
	LinkedHashMap<String, List<String>> impsMap;
	LinkedHashMap<String, List<String>> clicksMap;
	String impsFilePath = null;
	String clicksFilePath = null;
	String outputPath = null;
	List<String> filesList = null;
	String fileName;
	Logger logger = LoggerFactory.getLogger(Worker.class);

	public Worker(String impsPath, String clicksPath, HashMap<String, String> deviceTypeMap,
			HashMap<String, String> connectionTypeMap, String outputPath, String fileName) {

		impsFilePath = impsPath;
		clicksFilePath = clicksPath;
		this.deviceTypeMap = deviceTypeMap;
		this.connectionTypeMap = connectionTypeMap;
		this.outputPath = outputPath;
		this.fileName = fileName;
	}

	public void run() {

		try {
			logger.info("HOUR " + fileName.replace(".csv", "") + " ETL Start.");
			long current = new Date().getTime();
			// System.out.println(outputPath + " is been handled by : " +
			// Thread.currentThread().getName());

			impsReader = new CSVReader(new FileReader(impsFilePath));
			clicksReader = new CSVReader(new FileReader(clicksFilePath));

			impsMap = new LinkedHashMap<String, List<String>>();
			clicksMap = new LinkedHashMap<String, List<String>>();

			FileWriter fw = new FileWriter(outputPath.replace(".csv", ".json"), false);

			impsMap = processImps(impsFilePath);
			clicksMap = processClicks(impsMap);

			for (Map.Entry<String, List<String>> entry : impsMap.entrySet()) {
				fw.write(returnFinalString(entry.getValue()));
				fw.write("\n");
			}
			fw.close();
			logger.info("Storing output for HOUR " + fileName.replace(".csv", "") + " to directory : "
					+ outputPath.replace(".csv", ".json"));
			impsMap.clear();
			clicksMap.clear();
			long elapsed = new Date().getTime() - current;
			logger.info(
					"HOUR " + fileName.replace(".csv", "") + " ETL Completion, elapsed time milliseconds: " + elapsed);

		} catch (IOException e) {
			logger.error("Error: " + e.getMessage());
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unused")
	private void printHashMap(HashMap<String, List<String>> map) {
		for (Map.Entry<String, List<String>> entry : map.entrySet()) {
			System.out.print(entry.getKey() + " -> ");
			for (String string : entry.getValue()) {
				System.out.print(string + " ");
			}
			System.out.println();
		}
	}

	private LinkedHashMap<String, List<String>> processImps(String filePath) {
		String[] line = null;

		try {
			while ((line = impsReader.readNext()) != null) {
				if (line.length == 5) {
					line[0] = convertToISOTime(line[0]);
					line[2] = replaceString(connectionTypeMap, line[2]);
					line[3] = replaceString(deviceTypeMap, line[3]);

					List<String> list = new ArrayList<String>();
					list.addAll(Arrays.asList(line));
					String key = list.get(commonCol);
					list.remove(commonCol);

					impsMap.put(key, list);

				} else {
					logger.error("Problem with line: " + Arrays.asList(line) + " in " + impsFilePath);
				}
			}
		} catch (IOException e) {
			logger.error("Error: " + e.getMessage());
			e.printStackTrace();
		}
		return impsMap;
	}

	private LinkedHashMap<String, List<String>> processClicks(LinkedHashMap<String, List<String>> map) {
		String[] line2 = null;
		String key = null;
		try {
			while ((line2 = clicksReader.readNext()) != null) {

				if (line2.length == 3) {

					List<String> list = new ArrayList<String>();
					list.addAll(Arrays.asList(line2));

					if (!(list.size() == 1)) {
						key = list.get(commonCol);

						if (map.containsKey(key)) {

							List<String> list2 = map.get(key);
							list2.addAll(list);
							list2.remove(4);
							list2 = formatList(list2);

							map.put(key, list2);

						}
						// System.out.println(key + " added = " + list2);
					}
					clicksMap.put(key, list);
				} else {
					logger.error("Problem with line: " + Arrays.asList(line2) + " in " + clicksFilePath);
				}
			}
		} catch (IOException e) {
			logger.error("Error: " + e.getMessage());
			e.printStackTrace();
		}
		return map;
	}

	private List<String> formatList(List<String> list2) {
		if (!list2.isEmpty()) {
			list2.add(1, list2.get(4));
			list2.remove(5);
		} else
			throw new NullPointerException();
		return list2;
	}

	private String convertToISOTime(String string) {
		String str = null;
		if (string.isEmpty()) {
			throw new NullPointerException();
		}
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S'Z'");
		df.setTimeZone(TimeZone.getTimeZone("UTC"));

		try {
			str = df.format(new Date(Long.parseLong(string) * 1000));
		} catch (NumberFormatException e) {
			logger.error("Error: " + e.getMessage());
			e.printStackTrace();
		}
		return str;
	}

	private String replaceString(HashMap<String, String> map, String string) {
		if (map.containsKey(string))
			return map.get(string);
		else
			return string;
	}

	public String returnFinalString(List<String> list2) {

		LinkedHashMap<String, String> obj = new LinkedHashMap<String, String>();
		try {
			if (list2.isEmpty())
				throw new NullPointerException();
			else if (list2.size() < 6)
				throw new IndexOutOfBoundsException("Exception in thread \"" + Thread.currentThread().getName()
						+ "\" java.lang.IndexOutOfBoundsException when processing " + fileName + ": List size: "
						+ list2.size() + " , Expected Size: 6");

			obj.put("iso8601_timestamp", list2.get(0));
			obj.put("transaction_id", list2.get(1));
			obj.put("connection_type", list2.get(2).replaceAll("\"", ""));
			obj.put("device_type", list2.get(3).replaceAll("\"", ""));
			obj.put("imps", list2.get(4));
			obj.put("clicks", list2.get(5));
		} catch (IndexOutOfBoundsException e) {
			logger.error("Error:" + e.getMessage());
			e.printStackTrace();
		} catch (NullPointerException e) {
			logger.error("Error:" + e.getMessage());
			e.printStackTrace();
		}

		StringBuilder str = new StringBuilder("{");
		for (Map.Entry<String, String> entry : obj.entrySet()) {
			str.append("\"" + entry.getKey() + "\"" + ":\"" + entry.getValue() + "\",");
		}
		str.deleteCharAt(str.length() - 1);
		str.append("}");

		return str.toString();
	}

}
