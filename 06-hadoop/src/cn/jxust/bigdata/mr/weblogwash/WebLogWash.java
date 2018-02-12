package cn.jxust.bigdata.mr.weblogwash;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * web��־��ϴ
 */
public class WebLogWash {
	static class WebLogWashMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		Text k = new Text();
		NullWritable v = NullWritable.get();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			WebLogBean webLogBean = WebLogParaser.paraser(line);
			
			//���Բ���һ����̬��Դ���ˣ�.....��
			/*WebLogParser.filterStaticResource(webLogBean);*/
			
			if (!webLogBean.isValued()) return;//���ݲ�����Ͳ�д���ļ���ȥ
			k.set(webLogBean.toString());
			context.write(k, v);
		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(WebLogWash.class);

		job.setMapperClass(WebLogWashMapper.class);
		job.setNumReduceTasks(0);//����Ҫreduce
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, new Path("F:/mrdata/weblog/input"));
		FileOutputFormat.setOutputPath(job, new Path("F:/mrdata/weblog/output"));

		job.waitForCompletion(true);

	}
}
