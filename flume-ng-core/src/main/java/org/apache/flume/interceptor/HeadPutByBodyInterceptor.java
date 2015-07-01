package org.apache.flume.interceptor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class HeadPutByBodyInterceptor implements Interceptor {
	private static final Logger logger = LoggerFactory.getLogger(HeadPutByBodyInterceptor.class);
	private Pattern headLoopAppendPattern;
	private List<String> headCustomAppendFieldList;
	private List<Pattern> headCustomAppendPatternList;

	private HeadPutByBodyInterceptor(Pattern headLoopAppendPattern, List<String> headCustomAppendFieldList, List<Pattern> headCustomAppendPatternList) {
		this.headLoopAppendPattern = headLoopAppendPattern;
		this.headCustomAppendFieldList = headCustomAppendFieldList;
		this.headCustomAppendPatternList = headCustomAppendPatternList;
	}

	@Override
	public void initialize() {
		// TODO Auto-generated method stub

	}

	@Override
	public Event intercept(Event event) {
		StringBuilder bodybuilder = new StringBuilder(new String(event.getBody()));
		Map<String, String> headMap = event.getHeaders();
		
		if (headLoopAppendPattern != null) {
			Matcher matcher = headLoopAppendPattern.matcher(bodybuilder);
			// 覆盖
			while (matcher.find()) {
				headMap.put(matcher.group(1), matcher.group(2));
			}
		}

		if (headCustomAppendFieldList.size() > 0 && StringUtils.isNotBlank(headCustomAppendFieldList.get(0))) {
			int i;
			for (i = 0; i < headCustomAppendFieldList.size(); i++) {
				Matcher matcher = headCustomAppendPatternList.get(i).matcher(bodybuilder);
				if (matcher.find()) {
					headMap.put(headCustomAppendFieldList.get(i), matcher.group(1));
				} else {
					logger.debug("Don`t find '{}' in body", headCustomAppendFieldList.get(i));
				}
			}
		}
		event.setHeaders(headMap);
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
		private Pattern headLoopAppendPattern;
		private List<String> headCustomAppendFieldList;
		private List<Pattern> headCustomAppendPatternList = new ArrayList<Pattern>();

		@Override
		public Interceptor build() {
			return new HeadPutByBodyInterceptor(headLoopAppendPattern, headCustomAppendFieldList, headCustomAppendPatternList);
		}

		@Override
		public void configure(Context context) {
			String headLoopAppendRegex = context.getString("headLoopAppendRegex", "");
			if (StringUtils.isNotBlank(headLoopAppendRegex)) {
				headLoopAppendPattern = Pattern.compile(headLoopAppendRegex);
			}
			String headCustomAppendFields = context.getString("headCustomAppendFields", "");
			headCustomAppendFieldList = Arrays.asList(headCustomAppendFields.split("\\{split\\}"));
			String headCustomAppendRegexs = context.getString("headCustomAppendRegexs", "");
			String[] arrStr = headCustomAppendRegexs.split("\\{split\\}");
			Preconditions.checkArgument(headCustomAppendFieldList.size()==arrStr.length, "headCustomAppendFields` size is not equal to headCustomAppendRegexs`");
			for (String str : arrStr) {
				headCustomAppendPatternList.add(Pattern.compile(str));
			}
		}
	}
}
