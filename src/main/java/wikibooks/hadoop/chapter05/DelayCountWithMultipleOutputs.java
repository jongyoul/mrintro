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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DelayCountWithMultipleOutputs extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		String[] otherArgs = new GenericOptionsParser(getConf(), args)
				.getRemainingArgs();
		// �Է��� ������ ��� Ȯ��
		if (otherArgs.length != 2) {
			System.err
					.println("Usage: DelayCountWithMultipleOutputs <in> <out>");
			System.exit(2);
		}
		// Job �̸� ����
		Job job = new Job(getConf(), "DelayCountWithMultipleOutputs");

		// ����� ������ ��� ����
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		// Job Ŭ���� ����
		job.setJarByClass(DelayCountWithMultipleOutputs.class);
		// Mapper Ŭ���� ����
		job.setMapperClass(DelayCountMapperWithMultipleOutputs.class);
		// Reducer Ŭ���� ����
		job.setReducerClass(DelayCountReducerWithMultipleOutputs.class);

		// ����� ������ ���� ����
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// ���Ű �� ��°� ���� ����
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// MultipleOutputs ����
		MultipleOutputs.addNamedOutput(job, "departure",
				TextOutputFormat.class, Text.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "arrival", TextOutputFormat.class,
				Text.class, IntWritable.class);

		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		// Tool �������̽� ����
		int res = ToolRunner.run(new Configuration(),
				new DelayCountWithMultipleOutputs(), args);
		System.out.println("## RESULT:" + res);
	}
}
