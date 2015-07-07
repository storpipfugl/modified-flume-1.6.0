package org.flume.source.dirregex.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.flume.source.dirregex.DirRegexSource;
import org.junit.Before;
import org.junit.Test;

public class TestDirRegexSource {
	private Context context;
	private DirRegexSource source;
	private Channel channel;

	@Before
	public void before() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		context = new Context();
		context.put("monitorDir", "C:");
		context.put("monitorFileRegex", "cluster15-07-05.03.trs");
		context.put("checkFile", "C:\\check");
		context.put("contentRegex", "(<REC>(?:(?!\r\n\r\n)[\\W\\w])*)");
		
		Context context1 = new Context();
		context1.put("capacity", "20000");
		context1.put("transactionCapacity", "2000");

		source = new DirRegexSource();
		channel = new MemoryChannel();
		Configurables.configure(channel, context1);

		List<Channel> channels = new ArrayList<Channel>();
		channels.add(channel);

		ChannelSelector rcs = new ReplicatingChannelSelector();
		rcs.setChannels(channels);

		source.setChannelProcessor(new ChannelProcessor(rcs));
	}

	@Test
	public void testSink() throws InterruptedException {
		source.configure(context);
		source.start();
		Thread channelRunnable = new Thread(new ChannelRunnable());
		channelRunnable.start();
		MemoryRunnable memoryRunnable = new MemoryRunnable();
		memoryRunnable.run();
	}
	
	class ChannelRunnable implements Runnable {
		public void run() {
			while (true) {
				Transaction transaction = channel.getTransaction();
				transaction.begin();
				Event event;
				try {
					int num=0;
				    while ((event = channel.take()) != null){
						System.out.println(event);
						System.out.println(new String(event.getBody()));
						System.out.println("-------------------------");
						num++;
						if(num==20){
							break;
						}
				    }
				    transaction.commit();
				} catch (Throwable t) {
					transaction.rollback();
					System.out.println(t);
				} finally {
					transaction.close();
				}
			    try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	class MemoryRunnable implements Runnable {
		public void run() {
			while (true) {
				System.out.print("maxMemory:"+Runtime.getRuntime().maxMemory());
				System.out.print(",freeMemory:"+Runtime.getRuntime().freeMemory());
				System.out.println(",totalMemory:"+Runtime.getRuntime().totalMemory());
			    try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
}
