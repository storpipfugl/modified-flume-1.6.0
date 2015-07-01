package org.flume.source.sql;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
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
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class SqlSource extends AbstractSource implements EventDrivenSource, Configurable {
	private static final Logger logger = LoggerFactory.getLogger(SqlSource.class);
	private HibernateQuery hibernateQuery;
	private String tableUrl, table, columns, indexColumn, charsetName;
	private long delayTime;
	private int batchSize;
	private File checkFile;
	private Properties properties;
	private Properties tmpProperties = new Properties();
	private ScheduledExecutorService scheduledExecutorService;
	private ExecutorService executorService;

	private String DEFAULT_DIALECT = "org.hibernate.dialect.MySQLDialect";
	private String DEFAULT_DRIVERCLASSNAME = "com.mysql.jdbc.Driver";
	private String DEFAULT_USERNAME = "root";
	private String DEFAULT_PASSWORD = "root";
	private long DEFAULT_DELAYTIME = 30l;
	private int DEFAULT_BATCHSIZE = 1024;
	private String DEFAULT_CHARSETNAME = "UTF-8";

	public void configure(Context context) {
		logger.info("----------------------SQLSource configure...");

		// driverClassName,url,username,password,table,columns
		String dialect = context.getString("dialect", DEFAULT_DIALECT);
		Preconditions.checkArgument(StringUtils.isNotBlank(dialect), "Missing Param:'dialect'");
		String driverClassName = context.getString("driverClassName", DEFAULT_DRIVERCLASSNAME);
		Preconditions.checkArgument(StringUtils.isNotBlank(driverClassName), "Missing Param:'driverClassName'");
		String url = context.getString("url");
		Preconditions.checkArgument(StringUtils.isNotBlank(url), "Missing Param:'url'");
		String username = context.getString("username", DEFAULT_USERNAME);
		Preconditions.checkArgument(StringUtils.isNotBlank(username), "Missing Param:'username'");
		String password = context.getString("password", DEFAULT_PASSWORD);
		Preconditions.checkArgument(StringUtils.isNotBlank(password), "Missing Param:'password'");
		hibernateQuery = new HibernateQuery(dialect, driverClassName, url, username, password);

		table = context.getString("table");
		Preconditions.checkArgument(StringUtils.isNotBlank(table), "Missing Param:'table'");
		tableUrl = url + "/" + table;
		columns = context.getString("columns");
		Preconditions.checkArgument(StringUtils.isNotBlank(columns), "Missing Param:'columns'");
		indexColumn = context.getString("indexColumn");
		Preconditions.checkArgument(StringUtils.isNotBlank(indexColumn), "Missing Param:'indexColumn'");

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
		try {
			if (!checkFile.exists()) {
				checkFile.createNewFile();
			} else {
				properties.load(new FileInputStream(checkFile));
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		executorService = Executors.newCachedThreadPool();
		scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

		logger.info("----------------------SQLSource configured!");
	}

	public void start() {
		logger.info("----------------------SQLSource starting...");
		Runnable sqlRunnable = new SQLRunnable();
		scheduledExecutorService.scheduleWithFixedDelay(sqlRunnable, 0, delayTime, TimeUnit.SECONDS);
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
		super.stop();
		logger.info("----------------------SQLSource stopped!");
	}

	private class SQLRunnable implements Runnable {
		@Override
		public void run() {
			List<Map<String, Object>> objList = null;
			int readedIndex = 0;
			int tableIndex = hibernateQuery.selectMaxIndex(table, indexColumn);
			if (!properties.containsKey(tableUrl)) {
				objList = hibernateQuery.select(table, columns, indexColumn, 0);
				tmpProperties.put(tableUrl, tableIndex + "");
			} else {
				readedIndex = Integer.valueOf(properties.get(tableUrl).toString());
				// changed file
				if (!tmpProperties.containsKey(tableUrl) && readedIndex < tableIndex) {
					objList = hibernateQuery.select(table, columns, indexColumn, readedIndex);
					tmpProperties.put(tableUrl, tableIndex + "");
					// unchanged file
				} else if (!tmpProperties.containsKey(tableUrl) && readedIndex > tableIndex) {
					throw new IllegalStateException("'" + table + "'`s readedLength greater than tableLength");
				} else {
					return;
				}
			}
			logger.debug("----------------------table monitor start...");
			logger.info("----------------------read {}", table);
			logger.debug("----------------------get {} records", objList.size());

			try {
				// create events
				Set<String> tmpSet = null;
				List<Event> eventList = new ArrayList<Event>();
				List<Integer> indexList = new ArrayList<Integer>();
				for (Map<String, Object> map : objList) {
					Map<String, String> tmpMap = new HashMap<String, String>();
					indexList.add(Integer.valueOf(map.get("indexColumn").toString()));
					map.remove("indexColumn");
					Event event = EventBuilder.withBody("".getBytes());
					tmpSet = map.keySet();
					for (String str : tmpSet) {
						logger.debug("----------------------SqlSource events {}",map.get(str).toString());
						tmpMap.put(str, new String(map.get(str).toString().getBytes(charsetName),DEFAULT_CHARSETNAME));
						logger.debug("----------------------SqlSource events {}",tmpMap.get(str).toString());
					}
					event.setHeaders(tmpMap);
					event.getHeaders().put("tableUrl", tableUrl);
					eventList.add(event);
					// tmpMap.clear();
				}
				logger.debug("----------------------create {} events", eventList.size());

				if (eventList.size() == 0) {
					properties.put(tableUrl, tmpProperties.get(tableUrl));
					properties.store(new FileOutputStream(checkFile), null);
					return;
				}

				// process events
				int batchCount = eventList.size() / batchSize + 1;
				for (int i = 0; i < batchCount; i++) {
					if (i != batchCount - 1) {
						tmpProperties.put(tableUrl, indexList.get((i + 1) * batchSize - 1) + "");
						getChannelProcessor().processEventBatch(eventList.subList(i * batchSize, (i + 1) * batchSize));
					} else {
						tmpProperties.put(tableUrl, indexList.get(eventList.size() - 1) + "");
						getChannelProcessor().processEventBatch(eventList.subList(i * batchSize, eventList.size()));
					}
					properties.put(tableUrl, tmpProperties.get(tableUrl));
					properties.store(new FileOutputStream(checkFile), null);
				}
				logger.debug("----------------------process {} batchs", batchCount);
				logger.debug("----------------------table monitor stoped");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				tmpProperties.remove(tableUrl);
			}
		}
	}
}
