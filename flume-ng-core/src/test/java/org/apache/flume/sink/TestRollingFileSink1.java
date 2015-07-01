package org.apache.flume.sink;

import java.io.File;
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

public class TestRollingFileSink1 {
	private Channel channel = new PseudoTxnMemoryChannel();
	private File tmpDir = new File("/tmp/flume-rfs-" + System.currentTimeMillis() + "-" + Thread.currentThread().getId());
	private RollingFileSink sink = new RollingFileSink();
	
	@Test
	public void testSink() {
		Context context = new Context();
		context.put("sink.directory", tmpDir.getPath());
		context.put("sink.rollInterval", "0");
		context.put("sink.timeRollerFlag", "MINUTE");
		context.put("sink.filePrefix", "TRSSERVER");
		context.put("sink.formerField", "startTime");
		context.put("sink.sdfParsePattern", "yyyyMMddHHmm");

		Configurables.configure(sink, context);
		Configurables.configure(channel, new Context());

		sink.setChannel(channel);
		sink.start();
		
		Thread channelRunnablePush = new Thread(new ChannelRunnablePush());
		channelRunnablePush.start();
		Thread channelRunnablePush2 = new Thread(new ChannelRunnablePush2());
		channelRunnablePush2.start();
		Thread channelRunnablePoll = new Thread(new ChannelRunnablePoll());
		channelRunnablePoll.run();;
	}
	
	class ChannelRunnablePush implements Runnable {
		public void run() {
			int num = 0;
			while (true) {
				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
				for (int i = 0; i < 10; i++) {
					Event event = new SimpleEvent();
					event.setBody(("1-"+num + ":" + sdf.format(new Date())+"\r\r").getBytes());
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
					event.setBody(("2-"+num + ":" + sdfFormat.format(now)).getBytes());
					Map<String,String> map = new HashMap<String,String>();
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
