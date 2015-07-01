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
package org.apache.flume.sink.elasticsearch;

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

public class TestElasticSearchSink1 {
	private ElasticSearchSink sink = new ElasticSearchSink();
	private Channel channel = new PseudoTxnMemoryChannel();;

	@Test
	public void shouldIndexFiveEvents() throws Exception {
		Context context = new Context();
		context.put("type", "org.apache.flume.sink.elasticsearch.ElasticSearchSink");
		context.put("clusterName", "trsdc-elasticsearch-1.6");
		context.put("hostNames", "trsdc-01:9300,trsdc-02:9300,trsdc-03:9300,trsdc-04:9300");
		context.put("indexNameBuilder", "org.apache.flume.sink.elasticsearch.HeadFieldIndexNameBuilder");
		context.put("indexName", "radar");
		context.put("indexNameBuilder.timeRollerFlag", "DAY");
		context.put("indexNameBuilder.formerField", "time");
		context.put("indexNameBuilder.sdfParsePattern", "yyyyMMdd");
		sink = new ElasticSearchSink();
		sink.configure(context);

		Configurables.configure(channel, new Context());

		sink.setChannel(channel);
		sink.start();
		
		Thread channelRunnablePush = new Thread(new ChannelRunnablePush());
		channelRunnablePush.start();
		Thread channelRunnablePoll = new Thread(new ChannelRunnablePoll());
		channelRunnablePoll.run();
	}

	class ChannelRunnablePush implements Runnable {
		public void run() {
			int num = 0;
			while (true) {
				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
				SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMdd");
				for (int i = 0; i < 10; i++) {
					Event event = new SimpleEvent();
					Map<String,String> map = new HashMap<String,String>();
					map.put("time", sdf1.format(new Date()));
					map.put("Params", "(IR_SCREEN_NAME=�{之)");
					event.setHeaders(map);
					event.setBody(("1-" + num + ":" + sdf.format(new Date()) + "\r\r").getBytes());
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
