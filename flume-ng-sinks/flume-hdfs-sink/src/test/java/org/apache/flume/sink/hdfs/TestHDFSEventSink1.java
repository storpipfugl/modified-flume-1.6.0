package org.apache.flume.sink.hdfs;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.PseudoTxnMemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.SimpleEvent;
import org.junit.Test;

public class TestHDFSEventSink1 {
	private HDFSEventSink sink = new HDFSEventSink();
	private Channel channel = new PseudoTxnMemoryChannel();

	@Test
	public void start() throws IOException {
		System.setProperty("hadoop.home.dir", "D:\\hadoop-2.6.0");
		Context context = new Context();
		context.put("type", "hdfs");
		context.put("hdfs.path", "c:\\flume\\hdfs");
//		context.put("hdfs.path", "hdfs://trsdc-01:9000/");
	    context.put("hdfs.writeFormat", "Text");
		context.put("hdfs.filePrefix", "test");
		context.put("hdfs.rollCount", "0");
		context.put("hdfs.rollInterval", "0");
		context.put("hdfs.rollSize", "0");
		context.put("hdfs.batchSize", "1024");
		context.put("hdfs.fileType", "DataStream");
		context.put("hdfs.timeRollerFlag", "MINUTE");
		context.put("hdfs.formerField", "startTime");
		context.put("hdfs.sdfParsePattern", "yyyyMMddHHmm");
		sink.configure(context);
		Configurables.configure(channel, new Context());
		sink.setChannel(channel);
		sink.start();
		
//		Thread channelRunnablePush = new Thread(new ChannelRunnablePush());
//		channelRunnablePush.start();
		Thread channelRunnablePush2 = new Thread(new ChannelRunnablePush2());
		channelRunnablePush2.start();
		Thread channelRunnablePoll = new Thread(new ChannelRunnablePoll());
		channelRunnablePoll.run();
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
