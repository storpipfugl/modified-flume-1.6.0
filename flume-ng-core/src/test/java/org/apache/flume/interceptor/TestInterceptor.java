package org.apache.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.junit.Test;

import com.google.common.base.Charsets;

public class TestInterceptor {
	@Test
	public void testBasic() throws Exception {
		Interceptor.Builder builder = InterceptorBuilderFactory.newInstance(InterceptorType.BODYREPLACEBYBODY.toString());
		Context ctx = new Context();
		ctx.put("bodyRegex", "<\\w{7}>=\\d{4}([.])\\d{2}([.])\\d{2}[\\w\\W]*<\\w{7}>=\\d{4}([.])\\d{2}([.])\\d{2}");
		ctx.put("bodyStrs", "-{split}-{split}-{split}-");
	    builder.configure(ctx);
	    Interceptor interceptor = builder.build();
		Event event = EventBuilder.withBody("<CHECKCODE>=WIN-79AAT33SA5E-1426648233\n"+
"<TRANSID>=15\n"+
"<PROCESSCOSTTIME>=20\n"+
"<READCOUNT>=1\n"+
"<LOGDATE>=2015.07.04\n"+
"<LOGTIME>=2015.07.04 16:30:54+08:00\n"+
"<table>=trans_stat_split\n"+
"<WRITECOUNT>=13\n"+
"<READCOSTTIME>=3", Charsets.UTF_8);
		System.out.println(new String(interceptor.intercept(event).getBody()));

	}
}
