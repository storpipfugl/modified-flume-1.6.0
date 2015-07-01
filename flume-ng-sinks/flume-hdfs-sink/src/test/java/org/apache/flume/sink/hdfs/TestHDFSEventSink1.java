package org.apache.flume.sink.hdfs;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.channel.PseudoTxnMemoryChannel;
import org.apache.flume.channel.file.FileChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.SimpleEvent;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHDFSEventSink1 {
	private HDFSEventSink sink = new HDFSEventSink();
	private Channel channel = new PseudoTxnMemoryChannel();
	private static final Logger LOG = LoggerFactory.getLogger(HDFSEventSink.class);

	@Test
	public void start() throws IOException {

		FileChannel channel = new FileChannel();
		channel.setName("1");
		Context context0 = new Context();
		context0.put("checkpointDir", "C:\\flume\\flume1\\checkPoint1");
		context0.put("dataDirs", "C:\\flume\\flume1\\data1");
		Configurables.configure(channel, context0);
		Event event0 = new SimpleEvent();
		event0.setBody("加载java插件失败，名称=Debug_Parser_java".getBytes("gbk"));
		channel.start();
		Transaction tx = channel.getTransaction();
		tx.begin();
		channel.put(event0);
		tx.commit();
		tx.close();
		channel.stop();
		
		System.setProperty("hadoop.home.dir", "D:\\hadoop-2.6.0");
		LOG.debug("Starting...");
		Context context = new Context();
		// context.put("hdfs.path", "c:\\flume\\hdfs");
		context.put("hdfs.path", "hdfs://ui4:9000");
		context.put("hdfs.filePrefix", "TRSSERVER");
		context.put("hdfs.rollCount", "0");
		context.put("hdfs.rollInterval", "0");
		context.put("hdfs.rollSize", "0");
		context.put("hdfs.batchSize", "10");
		context.put("hdfs.writeFormat", "Text");
		context.put("hdfs.fileType", "DataStream");
//		context.put("hdfs.timeRollerFlag", "MINUTE");
//		context.put("hdfs.formerField", "startTime");
//		context.put("hdfs.sdfParsePattern", "yyyyMMddHHmm");
		
		FileChannel channel0 = new FileChannel();
		channel0.setName("1");
		Configurables.configure(channel0, context0);
		channel0.start();
		Configurables.configure(sink, context);
		sink.setChannel(channel0);
		sink.start();
		try {
			sink.process();
		} catch (EventDeliveryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		sink.stop();
		channel0.stop();
//		Thread channelRunnablePush = new Thread(new ChannelRunnablePush());
//		channelRunnablePush.start();
//		Thread channelRunnablePush2 = new Thread(new ChannelRunnablePush2());
//		channelRunnablePush2.start();
//		Thread channelRunnablePoll = new Thread(new ChannelRunnablePoll());
//		channelRunnablePoll.run();
	}

	class ChannelRunnablePush implements Runnable {
		public void run() {
			int num = 0;
			while (true) {
				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
				for (int i = 0; i < 10; i++) {
					Event event = new SimpleEvent();
					event.setBody(("1-" + num + ":" + sdf.format(new Date())).getBytes());
					channel.put(event);
					num++;
				}
				try {
					Thread.sleep(10000);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	class ChannelRunnablePush2 implements Runnable {
		public void run() {
			int num = 0;
			while (true) {
				SimpleDateFormat sdfFormat = new SimpleDateFormat("yyyyMMddHHmm");
				for (int i = 0; i < 10; i++) {
					Date now = new Date();
					Event event = new SimpleEvent();
					event.setBody(("2-" + num + ":" + sdfFormat.format(now)).getBytes());
					Map<String, String> map = new HashMap<String, String>();
					map.put("startTime", "201001010101");
					event.setHeaders(map);
					channel.put(event);
					num++;
				}
				try {
					Thread.sleep(4000);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	class ChannelRunnablePoll implements Runnable {
		public void run() {
			while (true) {
				try {
					sink.process();
					Thread.sleep(3000);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
}
