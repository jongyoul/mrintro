package wikibooks.hadoop.chapter07;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MapsideJoin extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		String[] otherArgs = new GenericOptionsParser(getConf(), args)
				.getRemainingArgs();
		// �Է��� ������ ��� Ȯ��
		if (otherArgs.length != 3) {
			System.err.println("Usage: MapsideJoin <metadata> <in> <out>");
			System.exit(2);
		}

		// Job �̸� ����
		Job job = new Job(getConf(), "MapsideJoin");

		// �л� ĳ�� ����
		DistributedCache.addCacheFile(new Path(otherArgs[0]).toUri(),
				job.getConfiguration());

		// ����� ������ ��� ����
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		// Job Ŭ���� ����
		job.setJarByClass(MapsideJoin.class);
		// Mapper ����
		job.setMapperClass(MapperWithMapsideJoin.class);
		// Reducer ����
		job.setNumReduceTasks(0);

		// ����� ������ ���� ����
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// ���Ű �� ��°� ���� ����
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		// Tool �������̽� ����
		int res = ToolRunner.run(new Configuration(), new MapsideJoin(), args);
		System.out.println("## RESULT:" + res);
	}
}
