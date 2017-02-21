package org.pcchen.pre;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import org.pcchen.bean.WebLogBean;

/**
 * File: WebLogParser.java 
 * Author:pcchen
 * Email：cpc_geek@163.com
 * Date: 2017年2月21日上午9:50:47
 * Description: 对数据的行进行指定字段读取转化清洗的类
 */

public class WebLogParser {
	public static void main(String[] args) {
		String line = "58.215.204.118 - - [18/Sep/2013:06:51:35 +0000] \"GET /wp-includes/js/jquery/jquery.js?ver=1.10.2 HTTP/1.1\" 304 0 \"http://blog.fens.me/nodejs-socketio-chat/\" \"Mozilla/5.0 (Windows NT 5.1; rv:23.0) Gecko/20100101 Firefox/23.0\"";
		WebLogBean parse = parse(line);
		System.out.println(parse);
	}
	
	public static WebLogBean parse(String line) {
		WebLogBean webLogBean = new WebLogBean();
		String[] arr = line.split(" ");
		
		if(arr.length > 11) {
			webLogBean.setRemote_addr(arr[0]);
			webLogBean.setRemote_user(arr[1]);
			String time_local = dateFormat(arr[3].substring(1));
			if(null==time_local || "".equals(time_local)) {
				time_local="-invalid_time-";
			}
			webLogBean.setTime_local(time_local);
			webLogBean.setRequest(arr[6]);
			webLogBean.setStatus(arr[8]);
			webLogBean.setBody_bytes_sent(arr[9]);
			webLogBean.setHttp_referer(arr[10]);
		
			//如果useragent的信息讲多，拼接userAgent
			if(arr.length > 12) {
				StringBuilder sb = new StringBuilder();
				for(int i = 11; i < arr.length; i++) {
					sb.append(arr[i]);
				}
				webLogBean.setHttp_user_agent(sb.toString());
			} else {
				webLogBean.setHttp_user_agent(arr[11]);
			}
		
			if(Integer.parseInt(webLogBean.getStatus()) >= 400) {
				webLogBean.setValid(false);
			}
			
			if("-invalid_time-".equals(webLogBean.getTime_local())) {
				webLogBean.setValid(false);
			}
		} else {
			return null;
		}
		return webLogBean;
	}
	
	public static String dateFormat(String timePattern) {
		DateFormat df = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
		Date date = null;
		try {
			date = df.parse(timePattern);
			return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US).format(date);
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
		
	}

	/**
	 * 过滤掉静态资源的访问页面
	 * @param webLogBean
	 * @param pageList
	 */
	public static void filtStaticResource(WebLogBean webLogBean,
			List<String> pageList) {
		if(!pageList.contains(webLogBean.getRequest())) {
			webLogBean.setValid(false);
		}
	}
}
