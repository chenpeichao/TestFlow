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
 * Email：cpc_geek@163.com
 * Date: 2017年2月22日上午9:35:50
 * Description:  
 * 		输入数据pageview模型结果数据
 * 		从pageview的数据中，进一步对数据进行清理，得到visit模型数据
 * 		主要包含访问进入时间、页面，离开时间、页面、以及ip等信息 
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
			
			//根据step对同一个session的点击流进行排序
			Collections.sort(pageViewList, new Comparator<PageViews>() {
				@Override
				public int compare(PageViews o1, PageViews o2) {
					return o1.getStep() - o2.getStep();
				}
			});
			
			//对排序后的集合的数据，进行整理、输出有效数据
			Visit visit = new Visit();
			//取visit的首记录
			visit.setInPage(pageViewList.get(0).getRequest());
			visit.setInTime(pageViewList.get(0).getTimeStr());
			//取visit的未记录
			visit.setOutPage(pageViewList.get(pageViewList.size()-1).getRequest());
			visit.setOutTime(pageViewList.get(pageViewList.size()-1).getTimeStr());
			//visit的访问页面数
			visit.setPageVisits(pageViewList.size());
			//visit的来访ip
			visit.setRemote_addr(pageViewList.get(0).getRemote_addr());
			//本次visit的referal
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
