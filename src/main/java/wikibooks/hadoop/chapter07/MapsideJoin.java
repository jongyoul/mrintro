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
		// 입력출 데이터 경로 확인
		if (otherArgs.length != 3) {
			System.err.println("Usage: MapsideJoin <metadata> <in> <out>");
			System.exit(2);
		}

		// Job 이름 설정
		Job job = new Job(getConf(), "MapsideJoin");

		// 분산 캐시 설정
		DistributedCache.addCacheFile(new Path(otherArgs[0]).toUri(),
				job.getConfiguration());

		// 입출력 데이터 경로 설정
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		// Job 클래스 설정
		job.setJarByClass(MapsideJoin.class);
		// Mapper 설정
		job.setMapperClass(MapperWithMapsideJoin.class);
		// Reducer 설정
		job.setNumReduceTasks(0);

		// 입출력 데이터 포맷 설정
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// 출력키 및 출력값 유형 설정
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		// Tool 인터페이스 실행
		int res = ToolRunner.run(new Configuration(), new MapsideJoin(), args);
		System.out.println("## RESULT:" + res);
	}
}
