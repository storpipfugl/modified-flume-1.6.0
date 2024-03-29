package org.flume.source.dirregex;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.ChannelException;
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

public class DirRegexSource extends AbstractSource implements EventDrivenSource, Configurable {
	private static final Logger logger = LoggerFactory.getLogger(DirRegexSource.class);
	private File monitorDir, checkFile;
	private Pattern monitorFilePattern, contentPattern;
	private long delayTime;
	private String charsetName;
	private int batchSize;
	private Properties properties;
	private Properties tmpProperties = new Properties();
	private ScheduledExecutorService scheduledExecutorService;
	private ExecutorService executorService;
	private SourceCounter sourceCounter;

	private String DEFAULT_MONITORFILEREGEX = "[\\W\\w]+";
	private String DEFAULT_CHARSETNAME = "UTF-8";
	private long DEFAULT_DELAYTIME = 30l;
	private int DEFAULT_BATCHSIZE = 1024;

	public void configure(Context context) {
		logger.info("----------------------DirRegexSource configure...");
		try {
			// monitorDir、monitorFileRegex
			String strMonitorDir = context.getString("monitorDir");
			Preconditions.checkArgument(StringUtils.isNotBlank(strMonitorDir), "Missing Param:'monitorDir'");
			String monitorFileRegex = context.getString("monitorFileRegex", DEFAULT_MONITORFILEREGEX);
			Preconditions.checkArgument(StringUtils.isNotBlank(monitorFileRegex), "Missing Param:'monitorFileRegex'");
			monitorFilePattern = Pattern.compile(monitorFileRegex);

			// checkFile
			String strCheckFile = context.getString("checkFile");
			Preconditions.checkArgument(StringUtils.isNotBlank(strCheckFile), "Missing Param:'checkFile'");

			// contentRegex
			String contentRegex = context.getString("contentRegex");
			Preconditions.checkArgument(StringUtils.isNotBlank(contentRegex), "Missing Param:'contentRegex'");
			contentPattern = Pattern.compile(contentRegex);

			// delayTime、charsetName、batchSize
			delayTime = context.getLong("delayTime", DEFAULT_DELAYTIME);
			Preconditions.checkArgument(delayTime > 0, "'delayTime' must be greater than 0");
			charsetName = context.getString("charsetName", DEFAULT_CHARSETNAME);
			Preconditions.checkArgument(StringUtils.isNotBlank(charsetName), "Missing Param:'charsetName'");
			batchSize = context.getInteger("batchSize", DEFAULT_BATCHSIZE);
			Preconditions.checkArgument(batchSize > 0, "'batchSize' must be greater than 0");

			monitorDir = new File(strMonitorDir);
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
		} catch (Exception e) {
			// TODO Auto-generated catch block
			throw new IllegalArgumentException(e);
		}
		logger.info("----------------------DirRegexSource configured!");
	}

	public void start() {
		logger.info("----------------------DirRegexSource starting...");
		sourceCounter.start();
		Runnable dirRunnable = new DirRunnable(monitorDir);
		scheduledExecutorService.scheduleWithFixedDelay(dirRunnable, 0, delayTime, TimeUnit.SECONDS);
		super.start();
		logger.info("----------------------DirRegexSource started!");
	}

	public void stop() {
		logger.info("----------------------DirRegexSource stopping...");
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
		logger.info("----------------------DirRegexSource stopped!");
	}

	private class DirRunnable implements Runnable {
		private File monitorDir;

		DirRunnable(File monitorDir) {
			this.monitorDir = monitorDir;
		}

		public void run() {
			logger.debug("----------------------dir monitor start...");
			monitorFile(monitorDir);
			logger.debug("----------------------dir monitor stoped");
		}

		private void monitorFile(File dir) {
			for (File tmpFile : dir.listFiles()) {
				if (tmpFile.isFile() && !tmpProperties.containsKey(tmpFile.getPath())) {
					Matcher matcher = monitorFilePattern.matcher(tmpFile.getName());
					if (matcher.matches()) {
						Runnable fileRunnable = null;
						// new file
						if (!properties.containsKey(tmpFile.getPath())) {
							fileRunnable = new FileRunnable(tmpFile, 0);
							tmpProperties.put(tmpFile.getPath(), "0");
						} else {
							int readedLength = Integer.valueOf(properties.get(tmpFile.getPath()).toString());
							// changed file
							if (readedLength < tmpFile.length()) {
								fileRunnable = new FileRunnable(tmpFile, readedLength);
								tmpProperties.put(tmpFile.getPath(), readedLength + "");
								// roll file
							} else if (readedLength > tmpFile.length()) {
								fileRunnable = new FileRunnable(tmpFile, 0);
								tmpProperties.put(tmpFile.getPath(), "0");
								// unchanged file
							} else {
								continue;
							}
						}
						executorService.submit(fileRunnable);
						continue;
					}
				} else if (tmpFile.isDirectory()) {
					monitorFile(tmpFile);
				}
			}
		}
	}

	private class FileRunnable implements Runnable {
		private File monitorFile;
		private int readedLength;

		FileRunnable(File monitorFile, int readedLength) {
			this.monitorFile = monitorFile;
			this.readedLength = readedLength;
		}

		public void run() {
			logger.debug("----------------------file monitor start...");
			logger.info("----------------------read {}", monitorFile.getPath());

			FileInputStream fis = null;
			try {
				// read file(read in batches)
				StringBuilder strBuilder = new StringBuilder();
				fis = new FileInputStream(monitorFile);
				int readed = 0;
				int mark = 0;
				fis.skip(readedLength);
				byte[] arrByte = new byte[1024 * 1024];
				long freeMemory = Runtime.getRuntime().freeMemory();
				while (readed + readedLength < monitorFile.length()) {
					int read = 0;
					while ((read = fis.read(arrByte)) != -1) {
						if (arrByte.length > read) {
							strBuilder.append(new String(arrByte, 0, read, charsetName));
						} else {
							strBuilder.append(new String(arrByte, charsetName));
						}
						readed += read;
						if (Runtime.getRuntime().totalMemory() == Runtime.getRuntime().maxMemory() || (Runtime.getRuntime().totalMemory() > Runtime.getRuntime().maxMemory() * 0.4 && Runtime.getRuntime().freeMemory() > freeMemory)) {
							freeMemory = Runtime.getRuntime().freeMemory();
							break;
						}
						freeMemory = Runtime.getRuntime().freeMemory();
					}
					logger.debug("----------------------get {} byte data", readed - readedLength);

					// create events(remove the last event)
					List<Integer> numList = new ArrayList<Integer>();
					Matcher contentMatcher = contentPattern.matcher(strBuilder.toString());
					List<Event> eventList = new ArrayList<Event>();
					while (contentMatcher.find()) {
						Event event = EventBuilder.withBody(contentMatcher.group(1).getBytes());
						event.getHeaders().put("filePath", monitorFile.getPath());
						eventList.add(event);
						numList.add(contentMatcher.end(1));
						mark = contentMatcher.start(1);
					}
					if (readed + readedLength < monitorFile.length() && eventList.size() > 0) {
						eventList.remove(eventList.size() - 1);
						numList.remove(numList.size() - 1);
					}
					logger.debug("----------------------create {} events", eventList.size());

					// process events(process in batches)
					if (eventList.size() != 0) {
						sourceCounter.addToEventReceivedCount(eventList.size());
						int batchCount = eventList.size() / batchSize + 1;
						try {
							for (int i = 0; i < batchCount; i++) {
								if (i != batchCount - 1) {
									tmpProperties.put(monitorFile.getPath(), (readedLength + numList.get((i + 1) * batchSize - 1)) + "");
									sourceCounter.addToEventAcceptedCount(eventList.subList(i * batchSize, (i + 1) * batchSize).size());
									getChannelProcessor().processEventBatch(eventList.subList(i * batchSize, (i + 1) * batchSize));
								} else {
									tmpProperties.put(monitorFile.getPath(), (readed + readedLength) + "");
									sourceCounter.addToEventAcceptedCount(eventList.subList(i * batchSize, eventList.size()).size());
									getChannelProcessor().processEventBatch(eventList.subList(i * batchSize, eventList.size()));
								}
							}
						} catch (ChannelException ex) {
							// TODO Auto-generated catch block
							ex.printStackTrace();
						}
						logger.debug("----------------------process {} batchs", batchCount);
					} else {
						tmpProperties.put(monitorFile.getPath(), (readed + readedLength) + "");
					}
					if (mark == 0) {
						strBuilder.setLength(0);
					} else {
						strBuilder.delete(0, mark);
					}
					logger.debug("----------------------file monitor stoped");
				}
				fis.close();
			} catch (Throwable e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				properties.put(monitorFile.getPath(), tmpProperties.get(monitorFile.getPath()));
				try {
					properties.store(new FileOutputStream(checkFile), null);
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				tmpProperties.remove(monitorFile.getPath());
			}
		}
	}
}
