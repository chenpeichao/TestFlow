package org.pcchen.pre;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.pcchen.bean.WebLogBean;

/**
 * File: WeblogPreMR.java 
 * Author:pcchen
 * Email：cpc_geek@163.com
 * Date: 2017年2月21日上午10:31:40
 * Description: 数据清洗mr:过滤出真是pv请求、转换时间格式、对缺失字段填充默认值，对数据惊醒valid和invalid标记
 */

public class WeblogPreMR {
	public static class WeblogPreMRMapper extends Mapper<LongWritable, Text, WebLogBean, NullWritable>{
		private List<String> pageList = new ArrayList<String>();
		
		@Override
		protected void setup( Context context)
				throws IOException, InterruptedException {
			pageList.add("/about");
			pageList.add("/black-ip-list/");
			pageList.add("/cassandra-clustor/");
			pageList.add("/finance-rhive-repurchase/");
			pageList.add("/hadoop-family-roadmap/");
			pageList.add("/hadoop-hive-intro/");
			pageList.add("/hadoop-zookeeper-intro/");
			pageList.add("/hadoop-mahout-roadmap/");
		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			WebLogBean webLogBean = WebLogParser.parse(line);
			if(webLogBean != null) {
				WebLogParser.filtStaticResource(webLogBean, pageList);
				if(!webLogBean.isValid()) {
					return;
				}
				context.write(webLogBean, NullWritable.get());
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(WebLogParser.class);
		
		job.setMapperClass(WeblogPreMRMapper.class);
		job.setOutputKeyClass(WebLogBean.class);
		job.setOutputValueClass(NullWritable.class);
		
		/*//hdfs的项目输出路径，如果存在，先删除
		Path outPath = new Path("");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outPath)) {
			fs.delete(outPath, true);
		}*/
		FileInputFormat.setInputPaths(job, new Path("F:\\change\\data\\clickflow\\pre_clear\\input"));
		FileOutputFormat.setOutputPath(job, new Path("F:\\change\\data\\clickflow\\pre_clear\\output"));
		job.setNumReduceTasks(0);
		boolean flag = job.waitForCompletion(true);
		
		System.exit(flag?0:1);
	}
}
