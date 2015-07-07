package org.apache.flume.sink.elasticsearch;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.junit.Assert.*;

import java.io.IOException;

import org.elasticsearch.common.xcontent.XContentBuilder;

public class Test {

	@org.junit.Test
	public void test() throws IOException {
		String str="<LOGCONTENT>=IR_LOADTIME:{20150702110157 TO 20150702110457] AND (IR_CONTENT:(`四川` OR `成都` OR `自贡` OR `攀枝花` OR `泸州` OR `德阳` OR `绵阳` OR `广元` OR `遂宁` OR `内江` OR `乐山` OR `南充` OR `宜宾` OR `广安` OR `达州` OR `巴中` OR `雅安` OR `眉山` OR `资阳` OR `阿坝` OR `甘孜` OR `凉山` OR `锦江` OR `青羊` OR `金牛` OR `武侯` OR `成华` OR `高新` OR `龙泉驿` OR `温江` OR `新都` OR `青白江` OR `双流` OR `郫县` OR `郫筒街道` OR `蒲江` OR `金堂` OR `新津` OR `都江堰` OR `彭州` OR `邛崃` OR `崇州`) AND IR_URLDATE:[2015.03.21 TO *] AND HYBASE_LOADTIME_A:[20150702094655 TO *]";
		XContentBuilder builder = jsonBuilder().startObject();
		builder.field("str", str);
		
	}

}
