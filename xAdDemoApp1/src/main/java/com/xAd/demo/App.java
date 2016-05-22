package com.xAd.demo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.InvalidPathException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;

import com.opencsv.CSVReader;

/*
  author- Ayush choukse 
*/
public class App {
	static CSVReader impsReader = null;
	static CSVReader clicksReader = null;
	static int commonCol = 1;
	static HashMap<String, String> deviceTypeMap;
	static HashMap<String, String> connectionTypeMap;
	static HashMap<String, List<String>> impsMap;
	static HashMap<String, List<String>> clicksMap;
	static Log logger = LogFactory.getLog(App.class);

	public static void main(String[] args) throws Exception {

		if (args.length < 4) {
			logger.error("Not Enough Parameters to run Application: See Usage.");
			System.exit(0);
		}
		updateLogFile(args[3]);
		logger.info("Storing logs at " + args[3]);
		logger.info("==========Starting-Application==========");
		logger.info("Parent directory :" + args[0]);

		String impsFilePath = args[0] + "facts/imps/";
		String clicksFilePath = args[0] + "facts/clicks/";
		String deviceTypePath = args[0] + "dimensions/device_type.json";
		String connectionTypePath = args[0] + "dimensions/connection_type.json";
		String outputPath = args[1];

		// check if path exists
		checkAbsolutePaths(impsFilePath, clicksFilePath, deviceTypePath, connectionTypePath, outputPath);

		// set to default value 5 if parallelism argument not passed.
		int parallelism = 5;
		if (args[2] != null)
			parallelism = Integer.parseInt(args[2]);

		// get map of all the deviceTypes and ConnectionType from their
		// respective files.
		deviceTypeMap = jsonParser(deviceTypePath);
		connectionTypeMap = jsonParser(connectionTypePath);

		// get List of all the files that are common in imps and clicks
		// directory.
		List<String> list = getFiles(impsFilePath, clicksFilePath);

		logger.info("Parallelism Level : " + parallelism);
		logger.info("Total No. of files to be processed : " + list.size());

		// set Thread pool to the parallelism value provided.
		ExecutorService executor = Executors.newFixedThreadPool(parallelism);
		for (int i = 0; i < list.size(); i++) {

			// create Task for processing and pass it to executorService to
			// schedule.
			Worker work1 = new Worker(impsFilePath + list.get(i), clicksFilePath + list.get(i), deviceTypeMap,
					connectionTypeMap, outputPath + list.get(i), list.get(i));
			executor.execute(work1);

		}
		executor.shutdown();

	}

	// private static void updateLog4jConfiguration(String string) throws
	// SecurityException, IOException {

	//// Properties props = new Properties();
	//// try {
	//// InputStream configStream = App.class.getResourceAsStream(
	//// "/log4j.properties");
	//// props.load(configStream);
	//// configStream.close();
	//// } catch (IOException e) {
	//// System.out.println("Errornot laod configuration file ");
	//// }
	//// props.setProperty("log4j.appender.FILE.file", string);
	//// LogManager.getLogManager().readConfiguration();
	////
	// LoggerContext logCtx = (LoggerContext) LoggerFactory.getILoggerFactory();
	//
	// PatternLayoutEncoder logEncoder = new PatternLayoutEncoder();
	// logEncoder.setContext(logCtx);
	// logEncoder.setPattern("%d{ISO8601} %-5p %m%n");
	// logEncoder.start();
	// RollingFileAppender logFileAppender = new RollingFileAppender();
	// logFileAppender.setContext(logCtx);
	// logFileAppender.setName("logFile");
	// logFileAppender.setEncoder(logEncoder);
	// logFileAppender.setAppend(false);
	// logFileAppender.setFile(string+"etl.log");
	// Logger log = logCtx.getLogger(App.class);
	//
	// log.addAppender(logFileAppender);
	private static void updateLogFile(String log) {
		Properties props = new Properties();
		try {
			InputStream configStream = App.class.getResourceAsStream("/log4j.properties");
			props.load(configStream);
			configStream.close();
		} catch (IOException e) {
			System.out.println("Error: Cannot laod configuration file ");
		}
		props.setProperty("log4j.rootLogger", "INFO, STDOUT, file");
		props.setProperty("log4j.appender.STDOUT", "org.apache.log4j.ConsoleAppender");
		props.setProperty("log4j.appender.STDOUT.layout", "org.apache.log4j.PatternLayout");
		props.setProperty("log4j.appender.STDOUT.layout.ConversionPattern", "%d{ISO8601} %-5p %m%n");
		props.setProperty("log4j.appender.file", "org.apache.log4j.RollingFileAppender");
		props.setProperty("log4j.appender.file.File", log);
		props.setProperty("log4j.appender.file.layout", "org.apache.log4j.PatternLayout");
		props.setProperty("log4j.appender.file.layout.ConversionPattern", "%d{ISO8601} %-5p %m%n");
		props.setProperty("log4j.appender.file.append", "false");

		LogManager.resetConfiguration();
		PropertyConfigurator.configure(props);
	}

	private static void checkAbsolutePaths(String impsFilePath, String clicksFilePath, String deviceTypePath,
			String connectionTypePath, String outputPath) {

		List<String> paths = new ArrayList<String>();
		paths.add(connectionTypePath);
		paths.add(deviceTypePath);
		paths.add(clicksFilePath);
		paths.add(impsFilePath);
		paths.add(outputPath);

		try {
			for (String string : paths) {
				if (!new File(string).exists())
					throw new InvalidPathException(string, "Incorrect Path provided or File not present: ");
			}
		} catch (InvalidPathException e) {
			logger.error("Error: " + e.getMessage());
			e.printStackTrace();
			System.exit(0);
		}
	}

	private static List<String> getFiles(String imps, String clicks) {

		List<String> impsFilesList = new ArrayList<String>();
		List<String> clicksFilesList = new ArrayList<String>();
		List<String> newList = new ArrayList<String>();

		// Create filter to getList of only .csv files.
		FilenameFilter textFilter = new FilenameFilter() {
			public boolean accept(File dir, String name) {
				if (name.toLowerCase().endsWith(".csv"))
					return true;
				else
					return false;
			}
		};

		File[] impsFiles = new File(imps).listFiles(textFilter);
		File[] clicksFiles = new File(clicks).listFiles(textFilter);

		// Adding filenames from each directory to an arraylist.
		for (File file : impsFiles) {
			if (file.isFile()) {
				if (!(file.length() == 0))
					impsFilesList.add(file.getName());
				else
					logger.error(file.getName() + " is empty in dir: " + file.getAbsolutePath());
			}
		}

		for (File file : clicksFiles) {
			if (file.isFile()) {
				if (!(file.length() == 0))
					clicksFilesList.add(file.getName());
				else
					logger.error(file.getName() + " is empty" + file.getAbsolutePath());
			}
		}

		if (impsFilesList.equals(clicksFilesList)) {
			return impsFilesList;
		} else {
			int temp = impsFilesList.size();

			if (clicksFilesList.size() < impsFilesList.size())
				temp = clicksFilesList.size();

			for (int i = 0; i < temp; i++) {
				// adding the files to be processed in a newList.
				if (impsFilesList.contains(clicksFilesList.get(i)))
					newList.add(clicksFilesList.get(i));

				// log message for the file not being processed.
				if (!impsFilesList.contains(clicksFilesList.get(i)))
					logger.info(clicksFilesList.get(i)
							+ " File either not present or empty in /in/imps/ and will not be processed.");
				else if (!clicksFilesList.contains(impsFilesList.get(i)))
					logger.info(impsFilesList.get(i)
							+ " File either not present or empty in /in/clicks/ and will not be processed.");
			}
			return newList;
		}
	}

	public static HashMap<String, String> jsonParser(String filePath) {

		HashMap<String, String> map = new HashMap<String, String>();
		JsonReader jsonReader = null;
		JsonArray jsonArray = null;
		try {
			jsonReader = Json.createReader(new FileInputStream(filePath));
		} catch (FileNotFoundException e) {
			logger.error("Error: " + e.getMessage());
			e.printStackTrace();
		}

		jsonArray = jsonReader.readArray();

		for (int i = 0; i < jsonArray.size(); i++) {
			JsonArray jobject = jsonArray.getJsonArray(i);
			map.put(jobject.get(0).toString(), jobject.get(1).toString());
		}
		// map.forEach((k, v) -> System.out.println(k + " " + v));
		return map;
	}

}
