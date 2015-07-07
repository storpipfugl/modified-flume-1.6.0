package org.apache.flume.interceptor;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BodyAppendByHeadInterceptor implements Interceptor {
	private static final Logger logger = LoggerFactory.getLogger(BodyAppendByHeadInterceptor.class);
	private String bodyLoopAppendFormat, bodyCustomAppendFormat;

	private BodyAppendByHeadInterceptor(String bodyLoopAppendFormat, String bodyCustomAppendFormat) {
		this.bodyLoopAppendFormat = bodyLoopAppendFormat;
		this.bodyCustomAppendFormat = bodyCustomAppendFormat;
	}

	@Override
	public void initialize() {
		// TODO Auto-generated method stub

	}

	@Override
	public Event intercept(Event event) {
		StringBuilder bodybuilder = new StringBuilder(new String(event.getBody()));
		Map<String, String> headMap = event.getHeaders();
		
		if (StringUtils.isNotBlank(bodyLoopAppendFormat)) {
			Set<String> keySet = headMap.keySet();
			for (String str : keySet) {
				bodybuilder.append(bodyLoopAppendFormat.replaceAll("\\{key\\}", str).replaceAll("\\{value\\}", Matcher.quoteReplacement(headMap.get(str).toString())));
			}
		}

		if (StringUtils.isNotBlank(bodyCustomAppendFormat)) {
			Pattern pattern = Pattern.compile("\\{([^\\}]*)\\}");
			Matcher matcher = pattern.matcher(bodyCustomAppendFormat);
			String bodyAppendStr = bodyCustomAppendFormat;
			while (matcher.find()) {
				if (headMap.containsKey(matcher.group(1))) {
					bodyAppendStr = bodyAppendStr.replaceAll("\\{" + matcher.group(1) + "\\}", headMap.get(matcher.group(1).toString()).toString());
				} else {
					logger.debug("Don`t have '{}' in head", matcher.group(1));
				}
			}
			bodybuilder.append(bodyAppendStr);
		}
		try {
			event.setBody(bodybuilder.toString().getBytes("utf-8"));
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
		private String bodyLoopAppendFormat, bodyCustomAppendFormat;

		@Override
		public Interceptor build() {
			return new BodyAppendByHeadInterceptor(bodyLoopAppendFormat, bodyCustomAppendFormat);
		}

		@Override
		public void configure(Context context) {
			bodyLoopAppendFormat = context.getString("bodyLoopAppendFormat","").replaceAll("\\{separator\\}", System.getProperty("line.separator"));
			bodyCustomAppendFormat = context.getString("bodyCustomAppendFormat","").replaceAll("\\{separator\\}", System.getProperty("line.separator"));
		}
	}
}
