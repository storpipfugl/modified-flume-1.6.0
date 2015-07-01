package org.flume.source.sql.test;

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
import org.apache.flume.interceptor.Interceptor;
import org.apache.flume.interceptor.InterceptorBuilderFactory;
import org.apache.flume.interceptor.InterceptorType;
import org.flume.source.sql.SqlSource;
import org.junit.Before;
import org.junit.Test;

public class TestSqlSource {

	private Context context;
	private SqlSource source;
	private Channel channel;
	private Interceptor interceptor1;
	private Interceptor interceptor2;

	@Before
	public void before() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		context = new Context();
		context.put("url", "jdbc:mysql://127.0.0.1:3306/das_log_base");
		context.put("table", "ckm_logs");
		context.put("columns", "CHECKCODE,TRANSID,LOGDATE,LOGTIME,LOGCONTENT,LOGTYPE");
		context.put("eventBody", "/LOGDATE /LOGTIME : /LOGCONTENT");
		context.put("checkFile", "C:\\flume\\check");
		context.put("indexColumn", "LOGID");
		context.put("charsetName", "GBK");
		
		Context context1 = new Context();
		context1.put("checkpointDir", "C:\\flume\\flume1\\checkPoint1");
		context1.put("dataDirs", "C:\\flume\\flume1\\data1");

		source = new SqlSource();
		channel = new MemoryChannel();
		Configurables.configure(channel, context1);

		List<Channel> channels = new ArrayList<Channel>();
		channels.add(channel);

		ChannelSelector rcs = new ReplicatingChannelSelector();
		rcs.setChannels(channels);

		source.setChannelProcessor(new ChannelProcessor(rcs));
		
		Context ctx = new Context();
		Interceptor.Builder builder = InterceptorBuilderFactory.newInstance(InterceptorType.HOST.toString());
		ctx.put("hostHeader", "hostName");
		builder.configure(ctx);
		interceptor1 = builder.build();
		Context ctx0 = new Context();
		Interceptor.Builder builder0 = InterceptorBuilderFactory.newInstance(InterceptorType.HEADPUTBYHEAD.toString());
		ctx0.put("headKeys", "params_length");
		ctx0.put("headValues", "{hostName.length}");
		builder0.configure(ctx0);
		interceptor2 = builder0.build();
	}

	@Test
	public void testSink() throws InterruptedException {
		source.configure(context);
		source.start();
		ChannelRunnable channelRunnable = new ChannelRunnable();
		channelRunnable.run();
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
				    	Event event1 = interceptor2.intercept(interceptor1.intercept(event));
						System.out.println(event1);
						System.out.println(new String(event1.getBody()));
						System.out.println("-------------------------");
						num++;
						if(num==20){
							break;
						}
				    }
				    transaction.commit();
				} catch (Throwable t) {
					transaction.rollback();
					t.printStackTrace();
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
}
