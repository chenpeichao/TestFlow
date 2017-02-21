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
 * Date:2017��2��21������9:54:49 			</br>
 * Copyright (c) 2017, cpc_geek@163.com All Rights Reserved. 
 * Description: ����ϴ֮���������־��������������pageviewsģ������
 * 	���ֳ�ÿһ�λػ�����ÿһ�ε�visit(session)���session-id�����uuid��
 * 	�����ÿһ�λỰ�������ʵ�ÿ��ҳ��(����ʱ�䣬url��ͣ��ʱ�����Լ���ҳ�������session�е����)
 * 	����referral_url, body_bytes_send,useragent
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
			//���ڲ����ϸ�ʽ�����ݽ��й���
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
			
			//��iterator�е�webLogBean���󣬱��������ҷ��ڼ����У�������������Լ����
			for(WebLogBean bean : iter) {
				WebLogBean webLogBean = new WebLogBean();
				try {
					BeanUtils.copyProperties(webLogBean, bean);
				} catch (Exception e) {
					e.printStackTrace();
				}
				webLogBeanList.add(webLogBean);
			}
			
			//�Է��뼯���е�weblogbean�������ָ���ֶν�������,�Է����ͬһ��session�µķ��ʽ���ͳ�ơ�����
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
			 * �����߼�Ϊ��������bean�зֱ������visit������һ��visit�������ʵ�page��˳����step
			 * ����˼�룺
			 * ���ǱȽ�����������¼�е�ʱ�����ʱ���<30���ӣ����������¼����ͬһ��session
			 * ���򣬾����ڲ�ͬ��session
			 * 
			 */
			//�Ƚ����η��ʵ�ʱ��������ѧ30���ӣ�����Ϊ���η���
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
				 * �������һ�����ݣ���������һ�����������ڶ���ʱ�����
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
				
				//���������һ����Ĭ��ͣ��60����
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
