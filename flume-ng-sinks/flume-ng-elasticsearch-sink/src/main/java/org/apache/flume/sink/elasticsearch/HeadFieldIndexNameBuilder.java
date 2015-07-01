/*
 * Copyright 2014 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flume.sink.elasticsearch;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;

import com.google.common.base.Preconditions;

public class HeadFieldIndexNameBuilder implements IndexNameBuilder {

	private String indexName, formerField, timeRollerFlag;
	private SimpleDateFormat sdfParse, sdfFormat;

	@Override
	public String getIndexName(Event event) {
		try {
			return indexName + sdfFormat.format(sdfParse.parse(event.getHeaders().get(formerField)));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		throw new IllegalStateException("HeadFieldIndexNameBuilder parse "+formerField+" error!");
	}

	@Override
	public String getIndexPrefix(Event event) {
		throw new IllegalStateException("HeadFieldIndexNameBuilder isn`t support IndexPrefix");
	}

	@Override
	public void configure(Context context) {
		indexName = context.getString(ElasticSearchSinkConstants.INDEX_NAME);
		// 历史读取
		formerField = context.getString("formerField");
		Preconditions.checkArgument(StringUtils.isNotBlank(formerField), "Missing Param:'indexNameBuilder.formerField'");
		String sdfParsePattern = context.getString("sdfParsePattern");
		Preconditions.checkArgument(StringUtils.isNotBlank(sdfParsePattern), "Missing Param:'indexNameBuilder.sdfParsePattern'");
		timeRollerFlag = context.getString("timeRollerFlag");
		Preconditions.checkArgument(StringUtils.isNotBlank(timeRollerFlag), "Missing Param:'indexNameBuilder.timeRollerFlag'");
		if ("DAY".equals(timeRollerFlag)) {
			sdfFormat = new SimpleDateFormat("yyyyMMdd");
		} else if ("HOUR".equals(timeRollerFlag)) {
			sdfFormat = new SimpleDateFormat("yyyyMMddHH");
		} else if ("MINUTE".equals(timeRollerFlag)) {
			sdfFormat = new SimpleDateFormat("yyyyMMddHHmm");
		}
		sdfParse = new SimpleDateFormat(sdfParsePattern);
	}

	@Override
	public void configure(ComponentConfiguration conf) {
	}
}
