package cn.jxust.bigdata.mr.combinefile;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/*
 * 自定义RecordReader
 * 
 * RecordReader的核心工作逻辑：
 * 通过nextKeyValue()方法去读取数据构造将返回的key   value
 * 通过getCurrentKey 和 getCurrentValue来返回上面构造好的key和value
 * 
 */
class WholeFileRecordReader extends RecordReader<NullWritable, BytesWritable> {//map的输入key value
	private FileSplit fileSplit;
	private Configuration conf;
	private BytesWritable value = new BytesWritable();//map的输入value  因为map输出key为文件名，所以map的输入就是NullWritable
	private boolean processed = false;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		this.fileSplit = (FileSplit) split;//初始化要切的文件并得到上下文context
		this.conf = context.getConfiguration();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!processed) {
			//构建一个数组缓冲区
			byte[] contents = new byte[(int) fileSplit.getLength()];
			Path file = fileSplit.getPath();
			FileSystem fs = file.getFileSystem(conf);
			FSDataInputStream in = null;
			try {
				in = fs.open(file);//得到输入流
				IOUtils.readFully(in, contents, 0, contents.length);
				value.set(contents, 0, contents.length);//赋值给value
			} finally {
				IOUtils.closeStream(in);
			}
			processed = true;
			return true;
		}
		return false;
	}

	
	
	
	@Override
	public NullWritable getCurrentKey() throws IOException,//map根据它来获取key value
			InterruptedException {
		return NullWritable.get();
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException,
			InterruptedException {
		return value;
	}

	/*
	 * 返回当前进度
	 */
	@Override
	public float getProgress() throws IOException {
		return processed ? 1.0f : 0.0f;
	}

	@Override
	public void close() throws IOException {
		// do nothing
	}
}