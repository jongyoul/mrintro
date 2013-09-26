package wikibooks.hadoop.chapter08;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import wikibooks.hadoop.chapter05.ArrivalDelayCountMapper;
import wikibooks.hadoop.chapter05.DelayCountReducer;

public class ArrivalDelayCountWithSnappy {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// Map ��� ���� ����
		conf.setBoolean("mapred.compress.map.output", true);
		conf.set("mapred.map.output.compression.codec",
				"org.apache.hadoop.io.compress.SnappyCodec");

		// �Է��� ������ ��� Ȯ��
		if (args.length != 2) {
			System.err
					.println("Usage: ArrivalDelayCountWithSnappy <input> <output>");
			System.exit(2);
		}
		// Job �̸� ����
		Job job = new Job(conf, "ArrivalDelayCountWithSnappy");

		// ����� ������ ��� ����
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Job Ŭ���� ����
		job.setJarByClass(ArrivalDelayCountWithSnappy.class);
		// Mapper Ŭ���� ����
		job.setMapperClass(ArrivalDelayCountMapper.class);
		// Reducer Ŭ���� ����
		job.setReducerClass(DelayCountReducer.class);

		// ����� ������ ���� ����
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		// ������ ���� ����
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressorClass(job,
				SnappyCodec.class);
		SequenceFileOutputFormat.setOutputCompressionType(job,
				CompressionType.BLOCK);

		// ���Ű �� ��°� ���� ����
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.waitForCompletion(true);
	}
}
