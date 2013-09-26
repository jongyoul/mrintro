package wikibooks.hadoop.chapter06;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SequenceFileCreator extends Configured implements Tool {
	static class DistanceMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, IntWritable, Text> {

		private IntWritable outputKey = new IntWritable();
		private int distance = 0;

		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {
			if (key.get() > 0) {
				try {
					String[] colums = value.toString().split(",");
					if (colums.length > 0) {
						if (!colums[18].equals("NA") && !colums[18].equals("")) {
							distance = Integer.parseInt(colums[18]);
						}
						outputKey.set(distance);
						output.collect(outputKey, value);
					}
				} catch (ArrayIndexOutOfBoundsException ae) {
					outputKey.set(0);
					output.collect(outputKey, value);
					ae.printStackTrace();
				} catch (Exception e) {
					outputKey.set(0);
					output.collect(outputKey, value);
					e.printStackTrace();
				}
			}
		}
	}

	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(SequenceFileCreator.class);
		conf.setJobName("SequenceFileCreator");

		conf.setMapperClass(DistanceMapper.class);
		conf.setNumReduceTasks(0);

		// ����� ��� ����
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		// ��� ������ SequenceFile�� ����
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		// ��� Ű�� �װ� ���� �Ÿ�(IntWritable)�� ����
		conf.setOutputKeyClass(IntWritable.class);
		// ��� ���� ����(Text)���� ����
		conf.setOutputValueClass(Text.class);

		// ���������� ���� ���� ����
		SequenceFileOutputFormat.setCompressOutput(conf, true);
		SequenceFileOutputFormat
				.setOutputCompressorClass(conf, GzipCodec.class);
		SequenceFileOutputFormat.setOutputCompressionType(conf,
				CompressionType.BLOCK);

		JobClient.runJob(conf);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new SequenceFileCreator(), args);
		System.out.println("## RESULT:" + res);
	}

}
