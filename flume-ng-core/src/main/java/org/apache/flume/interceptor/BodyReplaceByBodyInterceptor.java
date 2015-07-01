package org.apache.flume.interceptor;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;

import com.google.common.base.Preconditions;

public class BodyReplaceByBodyInterceptor implements Interceptor {
	private String[] bodyStrArr;
	private Pattern bodyPattern;

	private BodyReplaceByBodyInterceptor(Pattern bodyPattern, String[] bodyStrArr) {
		this.bodyPattern = bodyPattern;
		this.bodyStrArr = bodyStrArr;
	}

	@Override
	public void initialize() {
		// TODO Auto-generated method stub

	}

	@Override
	public Event intercept(Event event) {
		StringBuilder bodybuilder = new StringBuilder(new String(event.getBody()));
		Matcher matcher = bodyPattern.matcher(bodybuilder);
		if (matcher.find()) {
			int i = 0;
			for (; i < bodyStrArr.length; i++) {
				bodybuilder = bodybuilder.replace(matcher.start(i + 1), matcher.end(i + 1), bodyStrArr[i]);
			}
			try {
				event.setBody(bodybuilder.toString().getBytes("utf-8"));
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return event;
	}

	@Override
	public List<Event> intercept(List<Event> events) {
		for (Event event : events) {
			intercept(event);
		}
		return events;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	public static class Builder implements Interceptor.Builder {
		private String[] bodyStrArr;
		private Pattern bodyPattern;

		@Override
		public Interceptor build() {
			return new BodyReplaceByBodyInterceptor(bodyPattern, bodyStrArr);
		}

		@Override
		public void configure(Context context) {
			String bodyRegex = context.getString("bodyRegex");
			Preconditions.checkArgument(StringUtils.isNotBlank(bodyRegex), "Missing Param:'bodyRegex'");
			bodyRegex = bodyRegex.replaceAll("\\{separator\\}", System.getProperty("line.separator"));
			bodyPattern = Pattern.compile(bodyRegex);
			String bodyStrs = context.getString("bodyStrs", "").replaceAll("\\{separator\\}", System.getProperty("line.separator"));
			bodyStrArr = bodyStrs.split("\\{split\\}");
		}
	}
}
