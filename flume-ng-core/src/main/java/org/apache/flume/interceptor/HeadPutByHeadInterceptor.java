package org.apache.flume.interceptor;

import java.lang.reflect.Method;
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

public class HeadPutByHeadInterceptor implements Interceptor {
	private static final Logger logger = LoggerFactory.getLogger(HeadPutByHeadInterceptor.class);
	private String[] headKeyArr,headValueArr;

	private HeadPutByHeadInterceptor(String[] headKeyArr, String[] headValueArr) {
		this.headKeyArr = headKeyArr;
		this.headValueArr = headValueArr;
	}

	@Override
	public void initialize() {
		// TODO Auto-generated method stub

	}

	@Override
	public Event intercept(Event event) {
		Map<String,String> headMap = event.getHeaders();
		Pattern pattern = Pattern.compile("\\{([^\\}]*)\\}");
		int i=0;
		Matcher matcher = null;
		String headStr = null;
		for(;i<headKeyArr.length;i++){
			boolean flag = false;
			matcher = pattern.matcher(headValueArr[i]);
			headStr = headValueArr[i];
			while (matcher.find()) {
				String[] strArr = matcher.group(1).split("[.]");
				if (headMap.containsKey(strArr[0])) {
					//反射
					if(strArr.length>1){
						try {
							Method method = String.class.getMethod(strArr[1]);
							headStr = headStr.replaceAll("\\{" + matcher.group(1) + "\\}", method.invoke(headMap.get(strArr[0])).toString());
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					//普通修改
					}else{
						headStr = headStr.replaceAll("\\{" + strArr[0] + "\\}", headMap.get(strArr[0]));
					}
					flag = true;
				} else {
					logger.debug("Don`t have '{}' in head", strArr[0]);
				}
			}
			if(flag)
				headMap.put(headKeyArr[i], headStr);
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
		private String[] headKeyArr,headValueArr;

		@Override
		public Interceptor build() {
			return new HeadPutByHeadInterceptor(headKeyArr, headValueArr);
		}

		@Override
		public void configure(Context context) {
			String headKeys = context.getString("headKeys");
			Preconditions.checkArgument(StringUtils.isNotBlank(headKeys), "Missing Param:'headKeys'");
			headKeyArr = headKeys.split("\\{split\\}");
			String headValues = context.getString("headValues");
			Preconditions.checkArgument(StringUtils.isNotBlank(headValues), "Missing Param:'headValues'");
			headValueArr = headValues.split("\\{split\\}");
		}
	}
}
