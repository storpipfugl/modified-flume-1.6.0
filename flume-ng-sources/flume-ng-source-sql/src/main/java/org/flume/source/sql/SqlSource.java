package org.flume.source.sql;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class SqlSource extends AbstractSource implements EventDrivenSource, Configurable {
	private static final Logger logger = LoggerFactory.getLogger(SqlSource.class);
	private String charsetName, url, username, password;
	private String[] tableArr, indexColumnArr, columnsArr;
	private long delayTime;
	private int batchSize;
	private File checkFile;
	private Properties properties;
	private Properties tmpProperties = new Properties();
	private ScheduledExecutorService scheduledExecutorService;
	private ExecutorService executorService;
	private SourceCounter sourceCounter;

	private String DEFAULT_USERNAME = "root";
	private String DEFAULT_PASSWORD = "root";
	private long DEFAULT_DELAYTIME = 30l;
	private int DEFAULT_BATCHSIZE = 1024;
	private String DEFAULT_CHARSETNAME = "UTF-8";

	public void configure(Context context) {
		logger.info("----------------------SQLSource configure...");
		try {
			// url,username,password,table,columns
			url = context.getString("url");
			Preconditions.checkArgument(StringUtils.isNotBlank(url), "Missing Param:'url'");
			username = context.getString("username", DEFAULT_USERNAME);
			Preconditions.checkArgument(StringUtils.isNotBlank(username), "Missing Param:'username'");
			password = context.getString("password", DEFAULT_PASSWORD);
			Preconditions.checkArgument(StringUtils.isNotBlank(password), "Missing Param:'password'");

			String tables = context.getString("tables");
			Preconditions.checkArgument(StringUtils.isNotBlank(tables), "Missing Param:'tables'");
			tableArr = tables.split("\\{split\\}");
			String columns = context.getString("columns");
			Preconditions.checkArgument(StringUtils.isNotBlank(columns), "Missing Param:'columns'");
			columnsArr = columns.split("\\{split\\}");
			String indexColumns = context.getString("indexColumns");
			Preconditions.checkArgument(StringUtils.isNotBlank(indexColumns), "Missing Param:'indexColumns'");
			indexColumnArr = indexColumns.split("\\{split\\}");
			Preconditions.checkArgument(tableArr.length == indexColumnArr.length, " count oftables is not equal to count of indexColumns");

			// delayTime、charsetName
			delayTime = context.getLong("delayTime", DEFAULT_DELAYTIME);
			Preconditions.checkArgument(delayTime > 0, "'delayTime' must be greater than 0");
			batchSize = context.getInteger("batchSize", DEFAULT_BATCHSIZE);
			Preconditions.checkArgument(batchSize > 0, "'batchSize' must be greater than 0");
			charsetName = context.getString("charsetName", DEFAULT_CHARSETNAME).toUpperCase();
			Preconditions.checkArgument(StringUtils.isNotBlank(charsetName), "Missing Param:'charsetName'");

			// checkFile、properties
			String strCheckFile = context.getString("checkFile");
			Preconditions.checkArgument(StringUtils.isNotBlank(strCheckFile), "Missing Param:'checkFile'");
			checkFile = new File(strCheckFile);
			properties = new Properties();
			if (!checkFile.exists()) {
				checkFile.createNewFile();
			} else {
				properties.load(new FileInputStream(checkFile));
			}
			executorService = Executors.newCachedThreadPool();
			scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
			sourceCounter = new SourceCounter("DirRegexSource");
			Class.forName("com.mysql.jdbc.Driver");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			throw new IllegalArgumentException(e);
		}
		logger.info("----------------------SQLSource configured!");
	}

	public void start() {
		logger.info("----------------------SQLSource starting...");
		sourceCounter.start();
		Runnable urlRunnable = new UrlRunnable();
		scheduledExecutorService.scheduleWithFixedDelay(urlRunnable, 0, delayTime, TimeUnit.SECONDS);
		super.start();
		logger.info("----------------------SQLSource started!");
	}

	public void stop() {
		logger.info("----------------------SQLSource stopping...");
		scheduledExecutorService.shutdown();
		executorService.shutdown();
		try {
			scheduledExecutorService.awaitTermination(10L, TimeUnit.SECONDS);
			executorService.awaitTermination(10L, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		scheduledExecutorService.shutdownNow();
		executorService.shutdown();
		sourceCounter.stop();
		super.stop();
		logger.info("----------------------SQLSource stopped!");
	}

	private class UrlRunnable implements Runnable {
		public void run() {
			int i = 0;
			int readedIndex = 0;
			for (; i < tableArr.length; i++) {
				if (!tmpProperties.containsKey(tableArr[i])) {
					int maxIndex = 0;
					try {
						Connection conn = DriverManager.getConnection(url, username, password);
						Statement statement = conn.createStatement();
						ResultSet rs = statement.executeQuery("select MAX(" + indexColumnArr[i] + ") from " + tableArr[i]);
						rs.next();
						maxIndex = rs.getInt(1);
						rs.close();
						statement.close();
						conn.close();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					Runnable tableRunnable = null;
					// new table
					if (!properties.containsKey(tableArr[i])) {
						tableRunnable = new TableRunnable(i, 0);
						tmpProperties.put(tableArr[i], "0");
					} else {
						readedIndex = Integer.valueOf(properties.get(tableArr[i]).toString());
						// changed table
						if (readedIndex < maxIndex) {
							tableRunnable = new TableRunnable(i, readedIndex);
							tmpProperties.put(tableArr[i], readedIndex + "");
							// illegalState
						} else if (readedIndex > maxIndex) {
							throw new IllegalStateException("'" + tableArr[i] + "'`s readedIndex greater than maxIndex");
							// unchanged table
						} else {
							continue;
						}
					}
					executorService.submit(tableRunnable);
					continue;
				}
			}
		}
	}

	private class TableRunnable implements Runnable {
		private int index;
		private int readedIndex;

		TableRunnable(int index, int readedIndex) {
			this.index = index;
			this.readedIndex = readedIndex;
		}

		public void run() {
			logger.debug("----------------------table monitor start...");
			logger.info("----------------------read {}", tableArr[index]);
			try {
				Connection conn = DriverManager.getConnection(url, username, password);
				Statement statement = conn.createStatement();
				ResultSet rs = statement.executeQuery("select " + columnsArr[index] + "," + indexColumnArr[index] + " indexColumn  from " + tableArr[index] + " where " + indexColumnArr[index] + " > " + readedIndex);

				// create events
				List<Event> eventList = new ArrayList<Event>();
				List<Integer> indexList = new ArrayList<Integer>();
				String[] columnArr = columnsArr[index].split(",");

				long freeMemory = Runtime.getRuntime().freeMemory();
				while (rs.next()) {
					indexList.add(rs.getInt("indexColumn"));
					Event event = EventBuilder.withBody("".getBytes());
					Map<String, String> tmpMap = new HashMap<String, String>();
					for (String column : columnArr) {
						tmpMap.put(column, new String(rs.getObject(column).toString().getBytes(charsetName), DEFAULT_CHARSETNAME));
					}
					tmpMap.remove("indexColumn");
					event.setHeaders(tmpMap);
					event.getHeaders().put("table", tableArr[index]);
					eventList.add(event);

					if (Runtime.getRuntime().totalMemory() == Runtime.getRuntime().maxMemory() || (Runtime.getRuntime().totalMemory() > Runtime.getRuntime().maxMemory() * 0.4 && Runtime.getRuntime().freeMemory() > freeMemory)) {
						freeMemory = Runtime.getRuntime().freeMemory();
						process(eventList, indexList);
						eventList.clear();
						indexList.clear();
					}
				}
				process(eventList, indexList);
				rs.close();
				statement.close();
				conn.close();
				logger.debug("----------------------table monitor stoped");
			} catch (Throwable e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				properties.put(tableArr[index], tmpProperties.get(tableArr[index]));
				try {
					properties.store(new FileOutputStream(checkFile), null);
				} catch (FileNotFoundException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				tmpProperties.remove(tableArr[index]);
			}
		}

		private void process(List<Event> eventList, List<Integer> indexList) throws Throwable {
			try {
				logger.debug("----------------------create {} events", eventList.size());

				if (eventList.size() != 0) {
					// process events
					sourceCounter.addToEventReceivedCount(eventList.size());
					int batchCount = eventList.size() / batchSize + 1;
					for (int i = 0; i < batchCount; i++) {
						if (i != batchCount - 1) {
							tmpProperties.put(tableArr[index], indexList.get((i + 1) * batchSize - 1) + "");
							sourceCounter.addToEventAcceptedCount(eventList.subList(i * batchSize, (i + 1) * batchSize).size());
							getChannelProcessor().processEventBatch(eventList.subList(i * batchSize, (i + 1) * batchSize));
						} else {
							tmpProperties.put(tableArr[index], indexList.get(eventList.size() - 1) + "");
							sourceCounter.addToEventAcceptedCount(eventList.subList(i * batchSize, eventList.size()).size());
							getChannelProcessor().processEventBatch(eventList.subList(i * batchSize, eventList.size()));
						}
					}
					logger.debug("----------------------process {} batchs", batchCount);
				}
			} catch (Throwable e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
