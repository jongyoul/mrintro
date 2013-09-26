package wikibooks.hadoop.chapter05;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DelayCountWithCounter extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		String[] otherArgs = new GenericOptionsParser(getConf(), args)
				.getRemainingArgs();
		// �Է��� ������ ��� Ȯ��
		if (otherArgs.length != 2) {
			System.err.println("Usage: DelayCountWithCounter <in> <out>");
			System.exit(2);
		}
		// Job �̸� ����
		Job job = new Job(getConf(), "DelayCountWithCounter");

		// ����� ������ ��� ����
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		// Job Ŭ���� ����
		job.setJarByClass(DelayCountWithCounter.class);
		// Mapper Ŭ���� ����
		job.setMapperClass(DelayCountMapperWithCounter.class);
		// Reducer Ŭ���� ����
		job.setReducerClass(DelayCountReducer.class);

		// ����� ������ ���� ����
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// ���Ű �� ��°� ���� ����
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		// Tool �������̽� ����
		int res = ToolRunner.run(new Configuration(),
				new DelayCountWithCounter(), args);
		System.out.println("## RESULT:" + res);
	}
}
