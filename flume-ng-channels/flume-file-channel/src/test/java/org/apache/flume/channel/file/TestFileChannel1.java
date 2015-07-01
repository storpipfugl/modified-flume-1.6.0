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
package org.apache.flume.channel.file;

import static org.apache.flume.channel.file.TestUtils.*;
import static org.fest.reflect.core.Reflection.*;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurables;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.flume.channel.file.FileChannel.FileBackedTransaction;
import org.apache.flume.channel.file.FlumeEventQueue.InflightEventWrapper;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.event.SimpleEvent;

public class TestFileChannel1 extends TestFileChannelBase {

	private static final Logger LOG = LoggerFactory.getLogger(TestFileChannel1.class);

	@Before
	public void setup() throws Exception {
		super.setup();
	}

	@After
	public void teardown() {
		super.teardown();
	}

	@Test
	public void start() throws Exception {
		FileChannel channel = new FileChannel();
		channel.setName("1");
		Context context0 = new Context();
		context0.put("checkpointDir", "C:\\flume\\flume1\\checkPoint1");
		context0.put("dataDirs", "C:\\flume\\flume1\\data1");
		Configurables.configure(channel, context0);

		Event event0 = new SimpleEvent();
		event0.setBody("吃饭".getBytes("gb2312"));

		channel.start();
		Transaction tx = channel.getTransaction();
		tx.begin();
		channel.put(event0);
		tx.commit();
		tx.close();
		channel.stop();

		channel = new FileChannel();
		Configurables.configure(channel, context0);
		channel.setName("1");
		channel.start();
		Transaction transaction = channel.getTransaction();
		transaction.begin();
		Event event1 = channel.take();
		transaction.commit();
		transaction.close();
		System.out.println(new String(event1.getBody(), "gb2312"));
	}
}
