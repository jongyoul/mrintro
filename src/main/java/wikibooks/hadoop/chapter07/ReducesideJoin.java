package wikibooks.hadoop.chapter07;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ReducesideJoin extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		String[] otherArgs = new GenericOptionsParser(getConf(), args)
				.getRemainingArgs();
		// �Է��� ������ ��� Ȯ��
		if (otherArgs.length != 3) {
			System.err.println("Usage: ReducesideJoin <metadata> <in> <out>");
			System.exit(2);
		}

		// Job �̸� ����
		Job job = new Job(getConf(), "ReducesideJoin");

		// ��� ������ ��� ����
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		// Job Ŭ���� ����
		job.setJarByClass(ReducesideJoin.class);
		// Reducer ����
		job.setReducerClass(ReducerWithReducesideJoin.class);

		// ����� ������ ���� ����
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// ���Ű �� ��°� ���� ����
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// MultipleInputs ����
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]),
				TextInputFormat.class, CarrierCodeMapper.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
				TextInputFormat.class, MapperWithReducesideJoin.class);

		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		// Tool �������̽� ����
		int res = ToolRunner.run(new Configuration(), new ReducesideJoin(),
				args);
		System.out.println("## RESULT:" + res);
	}
}
