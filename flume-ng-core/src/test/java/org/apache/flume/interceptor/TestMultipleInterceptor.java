package org.apache.flume.interceptor;

import static org.junit.Assert.*;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.junit.Test;

import com.google.common.base.Charsets;

public class TestMultipleInterceptor {

	@Test
	public void test() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		Interceptor.Builder builder0 = InterceptorBuilderFactory.newInstance(InterceptorType.HEADAPPEND.toString());

		Context ctx0 = new Context();
		ctx0.put("headLoopAppendRegex", "<([^>]*)>=([^z]*)");
		ctx0.put("headCustomAppendFields", "recordDate");
		ctx0.put("headCustomAppendRegexs", "(\\d{4}-\\d{2}-\\d{2})");
		builder0.configure(ctx0);
		Interceptor interceptor0 = builder0.build();

		Context ctx = new Context();
		Interceptor.Builder builder = InterceptorBuilderFactory.newInstance(InterceptorType.BODYAPPEND.toString());
//		ctx.put("bodyLoopAppendFormat", "\n[{key}:{value}]");
		ctx.put("bodyCustomAppendFormat", "{separator}{num1}!={num2}{separator}{num2}!={num3}");
		builder.configure(ctx);
		Interceptor interceptor = builder.build();
		Event event = EventBuilder.withBody("<num1>=1zasdgabctrtyj<num2>=2014-15-13ztuabcjtf<num3>=3zdfabcbf",Charsets.UTF_8);
		Event e2 = interceptor.intercept(interceptor0.intercept(event));
		System.out.println(e2);
		System.out.println(new String(e2.getBody()));
	}

}
