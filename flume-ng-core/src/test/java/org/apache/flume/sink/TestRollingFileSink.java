/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume.sink;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.PseudoTxnMemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.lifecycle.LifecycleException;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRollingFileSink {

	private static final Logger logger = LoggerFactory.getLogger(TestRollingFileSink.class);
	private Channel channel;
	private File tmpDir;
	private RollingFileSink sink;

	@Before
	public void setUp() {
		tmpDir = new File("/tmp/flume-rfs-" + System.currentTimeMillis() + "-" + Thread.currentThread().getId());

		sink = new RollingFileSink();

		sink.setChannel(new MemoryChannel());

		tmpDir.mkdirs();
	}

	public void tearDown() {
		tmpDir.delete();
	}

	public void testLifecycle() throws InterruptedException, LifecycleException {
		Context context = new Context();

		context.put("sink.directory", tmpDir.getPath());

		Configurables.configure(sink, context);

		sink.start();
		sink.stop();
	}

	@Test
	public void testAppend() throws InterruptedException, LifecycleException, EventDeliveryException, IOException {

		Context context = new Context();

		context.put("sink.directory", tmpDir.getPath());
		context.put("sink.rollInterval", "0");
		context.put("sink.timeRollerFlag", "MINUTE");
		context.put("sink.filePrefix", "TRSSERVER");
		context.put("sink.formerField", "startTime");
		context.put("sink.sdfParsePattern", "yyyyMMddHHmm");

		Configurables.configure(sink, context);

		channel = new PseudoTxnMemoryChannel();
		Configurables.configure(channel, context);

		sink.setChannel(channel);
		sink.start();
		Thread channelRunnablePush = new Thread(new ChannelRunnablePush());
		channelRunnablePush.start();
		Thread channelRunnablePush2 = new Thread(new ChannelRunnablePush2());
		channelRunnablePush2.start();
		Thread channelRunnablePoll = new Thread(new ChannelRunnablePoll());
		channelRunnablePoll.run();
	}

	public void testAppend2() throws InterruptedException, LifecycleException, EventDeliveryException, IOException {

		Context context = new Context();

		context.put("sink.directory", tmpDir.getPath());
		context.put("sink.rollInterval", "0");
		context.put("sink.batchSize", "1");

		Configurables.configure(sink, context);

		Channel channel = new PseudoTxnMemoryChannel();
		Configurables.configure(channel, context);

		sink.setChannel(channel);
		sink.start();

		for (int i = 0; i < 10; i++) {
			Event event = new SimpleEvent();

			event.setBody(("Test event " + i).getBytes());

			channel.put(event);
			sink.process();

			Thread.sleep(500);
		}

		sink.stop();

		for (String file : sink.getDirectory().list()) {
			BufferedReader reader = new BufferedReader(new FileReader(new File(sink.getDirectory(), file)));

			String lastLine = null;
			String currentLine = null;

			while ((currentLine = reader.readLine()) != null) {
				lastLine = currentLine;
				logger.debug("Produced file:{} lastLine:{}", file, lastLine);
			}

			reader.close();
		}
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
