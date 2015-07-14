package org.flume.source.dirregex.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

public class TmpTest {

	public void test() {
		String str = "abc14abc1abc2222222222222222222abc3abc4abc5abc6abc7abc8abc9abc10abc11abc12abc13";
		Pattern pattern = Pattern.compile("(abc[\\d]*)");
		Matcher matcher = pattern.matcher(str);
		int eventIdealSize = 14;
		List<String> list = new ArrayList<String>();
		StringBuilder strBuilder = new StringBuilder();

		// 1
		matcher.find();
		matcher.group(1);
		int eventFindCount = eventIdealSize / Math.min(matcher.group(1).length(), eventIdealSize);
		int eventSize = eventFindCount * matcher.group(1).length();
		strBuilder.append(matcher.group(1));

		// 2-n
		while (matcher.find()) {
			if (strBuilder.length() >= eventSize) {
				list.add(strBuilder.toString());
				strBuilder.setLength(0);
			}
			strBuilder.append(matcher.group(1));
		}
		list.add(strBuilder.toString());

		System.out.println(list);
	}

	public void testRead() {
		StringBuilder strBuilder = new StringBuilder();
		try {
			FileInputStream fis = new FileInputStream(new File("C:\\log150629.224530.trs"));
			byte[] arrByte = new byte[1024 * 1024];
			int read = 0;
			fis.skip(2470191);
			while ((read = fis.read(arrByte)) != -1) {
				if (arrByte.length > read) {
					strBuilder.append(new String(arrByte, 0, read, "gb18030"));
				} else
					strBuilder.append(new String(arrByte, "gb18030"));
			}
			fis.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			throw new IllegalStateException("File is Exceptional!");
		}
		try {
			System.out.println(new String(strBuilder.substring(0, 10).getBytes("utf-8")));
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void testRegex() {
		String str = "sf<REC><Message1>=type:search\r\n<Message>=type:search\r\n\nasdasdas\r\nsdfsa<REC><Message2>=type:search\r\n<Message>=type:search\r\n\nasdasdas\r\n";
		// "+System.getProperty("line.separator")+System.getProperty("line.separator")+"
		Pattern pattern = Pattern.compile("(<REC>(?:<[^>]*>=(?:(?!\r\n)[\\W\\w])*\r\n)+)");
		// System.out.println(matcher.matches());
		int a=0;
		StringBuilder bodybuilder = new StringBuilder(str);
		for(int i=0;i<2;i++){
			Matcher matcher = pattern.matcher(bodybuilder);
			List<String> eventList = new ArrayList<String>();
			while (matcher.find()) {
				a=matcher.start(1);
				eventList.add(matcher.group(1));
			}
			if(i!=1){
				eventList.remove(eventList.size()-1);
			}
			System.out.println(eventList);
			bodybuilder.delete(0, a);
			bodybuilder.append("sf<REC><Message3>=type:search\r\n<Message>=type:search\r\n");
		}
	}

	public void testRegex2() throws UnknownHostException, ClassNotFoundException, NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchFieldException {
		StringBuilder bodybuilder = new StringBuilder("<Message>=size:567");
		String[] bodyStrArr = new String[] { "" };
		Pattern bodyPattern = Pattern.compile("<Message>=(size:)");
		Matcher matcher = bodyPattern.matcher(bodybuilder);
		matcher.find();
		int i = 0;
		for (; i < bodyStrArr.length; i++) {
			bodybuilder = bodybuilder.replace(matcher.start(i + 1), matcher.end(i + 1), bodyStrArr[i]);
		}
		System.out.println(bodybuilder);
	}

	public void test1(){
		try {
			System.out.println(new String(new String("髙之".getBytes("gb2312"),"gb2312").getBytes("utf-8")));
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
