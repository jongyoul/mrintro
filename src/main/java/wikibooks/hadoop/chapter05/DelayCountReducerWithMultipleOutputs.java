package wikibooks.hadoop.chapter05;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class DelayCountReducerWithMultipleOutputs extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	private MultipleOutputs<Text, IntWritable> mos;

	// reduce ���Ű
	private Text outputKey = new Text();

	// reduce ��°�
	private IntWritable result = new IntWritable();

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs<Text, IntWritable>(context);
	}

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		// �޸� ������ �и�
		String[] colums = key.toString().split(",");

		// ���Ű ����
		outputKey.set(colums[1] + "," + colums[2]);

		// ��� ����
		if (colums[0].equals("D")) {
			// ����Ƚ�� �ջ�
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			// ��°� ����
			result.set(sum);
			// ��� ������ ����
			mos.write("departure", outputKey, result);
			// ���� ����
		} else {
			// ����Ƚ�� �ջ�
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			// ��°� ����
			result.set(sum);
			// ��� ������ ����
			mos.write("arrival", outputKey, result);
		}
	}

	@Override
	public void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
	}
}
