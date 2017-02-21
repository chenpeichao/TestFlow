package org.pcchen.clickstream;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.pcchen.bean.PageViews;
import org.pcchen.bean.WebLogBean;

/** 
 * Project Name:TestFlow </br>
 * File Name:PageViewsMR.java 		</br>
 * Date:2017年2月21日下午9:54:49 			</br>
 * Copyright (c) 2017, cpc_geek@163.com All Rights Reserved. 
 * Description: 将清洗之后的数据日志进行梳理出点击流pageviews模型数据
 * 	区分出每一次回话，给每一次的visit(session)添加session-id（随机uuid）
 * 	梳理出每一次会话中所访问的每个页面(请求时间，url，停留时长，以及该页面在这次session中的序号)
 * 	保留referral_url, body_bytes_send,useragent
*/
public class PageViewsMR {
	public static class PageViewsMRMapper extends Mapper<LongWritable, Text, Text, WebLogBean> {
		private WebLogBean webLogBean = new WebLogBean();
		private Text v = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split("\001");
			//对于不符合格式的数据进行过滤
			if(fields.length < 9) {
				return;
			}
			
			if("true".equals(fields[0])) {
				webLogBean.set(true, fields[1], fields[2], fields[3], fields[4], fields[5], fields[6], fields[7], fields[8]);
				v.set(fields[1]);
				context.write(v, webLogBean);
			}
		}
	}
	
	public static class PageViewsMRReduce extends Reducer<Text, WebLogBean, Text, NullWritable> {
		private PageViews pageViews = new PageViews();
		@Override
		protected void reduce(Text key, Iterable<WebLogBean> iter, Context context) throws IOException, InterruptedException {
			List<WebLogBean> webLogBeanList = new ArrayList<WebLogBean>();
			
			//将iterator中的webLogBean对象，遍历，并且放在集合中，方便后面排序以及输出
			for(WebLogBean bean : iter) {
				WebLogBean webLogBean = new WebLogBean();
				try {
					BeanUtils.copyProperties(webLogBean, bean);
				} catch (Exception e) {
					e.printStackTrace();
				}
				webLogBeanList.add(webLogBean);
			}
			
			//对放入集合中的weblogbean对象根据指定字段进行排序,以方便对同一个session下的访问进行统计、区分
			Collections.sort(webLogBeanList, new Comparator<WebLogBean>() {
				@Override
				public int compare(WebLogBean webLogBean1, WebLogBean webLogBean2) {
					Date d1 = toDate(webLogBean1.getTime_local());
					Date d2 = toDate(webLogBean2.getTime_local());
					if(d1 == null || d2 == null) {
						return 0;
					}
					return  d1.compareTo(d2);
				}
			});
			
			/**
			 * 以下逻辑为：从有序bean中分辨出各次visit，并对一次visit中所访问的page按顺序标号step
			 * 核心思想：
			 * 就是比较相邻两条记录中的时间差，如果时间差<30分钟，则该两条记录属于同一个session
			 * 否则，就属于不同的session
			 * 
			 */
			//比较两次访问的时间间隔，大学30分钟，则视为两次访问
			UUID sessionId = UUID.randomUUID();
			int step = 1;
			for(int i = 0; i < webLogBeanList.size(); i++) {
				if(webLogBeanList.size() == 1) {
					key.set(sessionId  + "\001" +
							webLogBeanList.get(0).getRemote_addr() + "\001" + 
							webLogBeanList.get(0).getRemote_user() + "\001" + 
							webLogBeanList.get(0).getTime_local() + "\001" + 
							webLogBeanList.get(0).getRequest() + "\001" + 
							(60) + "\001" + 
							webLogBeanList.get(0).getHttp_referer() + "\001" + 
							webLogBeanList.get(0).getHttp_user_agent() + "\001" + 
							webLogBeanList.get(0).getBody_bytes_sent() + "\001" + 
							webLogBeanList.get(0).getStatus() + "\001" + 
							step);
					context.write(key, NullWritable.get());
					break;
				}
				
				/**
				 * 如果大于一条数据，则跳过第一条，遍历到第二条时在输出
				 */
				if(i == 0) {
					continue;
				}
				
				long timeDiff = timeDiff(webLogBeanList.get(i).getTime_local(), webLogBeanList.get(i-1).getTime_local());
				if(timeDiff < 30 * 60 * 1000) {
					key.set(sessionId  + "\001" +
							webLogBeanList.get(i-1).getRemote_addr() + "\001" + 
							webLogBeanList.get(i-1).getRemote_user() + "\001" + 
							webLogBeanList.get(i-1).getTime_local() + "\001" + 
							webLogBeanList.get(i-1).getRequest() + "\001" + 
							(timeDiff / 1000) + "\001" + 
							webLogBeanList.get(i-1).getHttp_referer() + "\001" + 
							webLogBeanList.get(i-1).getHttp_user_agent() + "\001" + 
							webLogBeanList.get(i-1).getBody_bytes_sent() + "\001" + 
							webLogBeanList.get(i-1).getStatus() + "\001" + 
							step);
					context.write(key, NullWritable.get());
					step++;
				} else {
					key.set(sessionId  + "\001" +
							webLogBeanList.get(i-1).getRemote_addr() + "\001" + 
							webLogBeanList.get(i-1).getRemote_user() + "\001" + 
							webLogBeanList.get(i-1).getTime_local() + "\001" + 
							webLogBeanList.get(i-1).getRequest() + "\001" + 
							(60) + "\001" + 
							webLogBeanList.get(i-1).getHttp_referer() + "\001" + 
							webLogBeanList.get(i-1).getHttp_user_agent() + "\001" + 
							webLogBeanList.get(i-1).getBody_bytes_sent() + "\001" + 
							webLogBeanList.get(i-1).getStatus() + "\001" + 
							step);
					context.write(key, NullWritable.get());
					sessionId = UUID.randomUUID();
					step = 1;
				}
				
				//遍历到最后一条，默认停留60分钟
				if(i == webLogBeanList.size() - 1) {
					key.set(sessionId  + "\001" +
							webLogBeanList.get(i-1).getRemote_addr() + "\001" + 
							webLogBeanList.get(i).getRemote_user() + "\001" + 
							webLogBeanList.get(i).getTime_local() + "\001" + 
							webLogBeanList.get(i).getRequest() + "\001" + 
							(60) + "\001" + 
							webLogBeanList.get(i).getHttp_referer() + "\001" + 
							webLogBeanList.get(i).getHttp_user_agent() + "\001" + 
							webLogBeanList.get(i).getBody_bytes_sent() + "\001" + 
							webLogBeanList.get(i).getStatus() + "\001" +
							step);
					context.write(key, NullWritable.get());
				}
			}
		}
		private long timeDiff(String time_local, String time_local2) {
			return toDate(time_local).getTime() - toDate(time_local2).getTime();
		}
		private Date toDate(String time_local) {
			try {
				return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time_local);
			} catch (ParseException e) {
				e.printStackTrace();
				return null;
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(PageViewsMR.class);
		job.setMapperClass(PageViewsMRMapper.class);
		job.setReducerClass(PageViewsMRReduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(WebLogBean.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("F:\\change\\demo_data\\mr\\click_flow\\clear\\output"));
		FileOutputFormat.setOutputPath(job, new Path("F:\\change\\demo_data\\mr\\click_flow\\page_view\\output"));
		
		boolean flag = job.waitForCompletion(true);
		System.exit(flag?0:1);
	}
}
