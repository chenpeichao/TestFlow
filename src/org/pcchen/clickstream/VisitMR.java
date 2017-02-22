package org.pcchen.clickstream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
import org.pcchen.bean.Visit;

/**
 * File: VisterMR.java 
 * Author:pcchen
 * Email��cpc_geek@163.com
 * Date: 2017��2��22������9:35:50
 * Description:  
 * 		��������pageviewģ�ͽ������
 * 		��pageview�������У���һ�������ݽ��������õ�visitģ������
 * 		��Ҫ�������ʽ���ʱ�䡢ҳ�棬�뿪ʱ�䡢ҳ�桢�Լ�ip����Ϣ 
 */

public class VisitMR {
	public static class VisitMRMapper extends Mapper<LongWritable, Text, Text, PageViews> {
		private PageViews pageViews = new PageViews();
		private Text textKey = new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split("\001");
			if(fields.length > 9) {
				int step = Integer.parseInt(fields[10]);
				pageViews.set(fields[0], fields[1], fields[3], fields[4], step, fields[5], fields[6], fields[7], fields[8], fields[9]);
				textKey.set(fields[0]);
				context.write(textKey, pageViews);
			}
		}
	}
	
	public static class VisitMRReduce extends Reducer<Text, PageViews, NullWritable, Visit> {
		@Override
		protected void reduce(Text textKey, Iterable<PageViews> iter, Context context)
				throws IOException, InterruptedException {
			List<PageViews> pageViewList = new ArrayList<PageViews>();
			for(PageViews bean : iter) {
				PageViews pageViews = new PageViews();
				
				try {
					BeanUtils.copyProperties(pageViews, bean);
				} catch (Exception e) {
					e.printStackTrace();
				}
				pageViewList.add(pageViews);
			}
			
			//����step��ͬһ��session�ĵ������������
			Collections.sort(pageViewList, new Comparator<PageViews>() {
				@Override
				public int compare(PageViews o1, PageViews o2) {
					return o1.getStep() - o2.getStep();
				}
			});
			
			//�������ļ��ϵ����ݣ��������������Ч����
			Visit visit = new Visit();
			//ȡvisit���׼�¼
			visit.setInPage(pageViewList.get(0).getRequest());
			visit.setInTime(pageViewList.get(0).getTimeStr());
			//ȡvisit��δ��¼
			visit.setOutPage(pageViewList.get(pageViewList.size()-1).getRequest());
			visit.setOutTime(pageViewList.get(pageViewList.size()-1).getTimeStr());
			//visit�ķ���ҳ����
			visit.setPageVisits(pageViewList.size());
			//visit������ip
			visit.setRemote_addr(pageViewList.get(0).getRemote_addr());
			//����visit��referal
			visit.setReferal(pageViewList.get(0).getReferal());
			visit.setSession(pageViewList.get(0).getSession());
			
			context.write(NullWritable.get(), visit);
		}
	} 
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(VisitMR.class);
		job.setMapperClass(VisitMRMapper.class);
		job.setReducerClass(VisitMRReduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PageViews.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Visit.class);
		
		Path outPath = new Path("F:\\change\\demo_data\\mr\\click_flow\\visit\\output");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outPath)) {
			fs.delete(outPath, true);
		}
		
		FileInputFormat.setInputPaths(job, new Path("F:\\change\\demo_data\\mr\\click_flow\\page_view\\output"));
		FileOutputFormat.setOutputPath(job, outPath);
		
		boolean flag = job.waitForCompletion(true);
		
		System.exit(flag?0:1);
	}
}
