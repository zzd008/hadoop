package cn.jxust.bigdata.mr.combinefile;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/*
 * 自定义inputformat 需要自定义一个RecordReader
 * 先切片getsplit()--->得到RecordReader createRecordReader()--->调用RecordReader中的nextkeyvalue()去读数据
 * 
 */
public class WholeFileInputFormat extends FileInputFormat<NullWritable, BytesWritable>{

	@Override
	protected boolean isSplitable(JobContext context, Path file) {//文件是否切片，因为是小文件所以不切
		return false;
	}

	@Override
	public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException,InterruptedException {
		
		WholeFileRecordReader reader = new WholeFileRecordReader();
		reader.initialize(split, context);//把切片文件和context初始化给RecordReader
		return reader;
		
	}

}
