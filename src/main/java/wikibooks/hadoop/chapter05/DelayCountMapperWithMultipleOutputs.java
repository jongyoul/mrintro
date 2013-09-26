package wikibooks.hadoop.chapter05;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DelayCountMapperWithMultipleOutputs extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	// map ��°�
	private final static IntWritable outputValue = new IntWritable(1);

	// map ���Ű
	private Text outputKey = new Text();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		if (key.get() > 0) {
			// �޸� ������ �и�
			String[] colums = value.toString().split(",");
			if (colums != null && colums.length > 0) {
				try {
					// ��� ���� ������ ���
					if (!colums[15].equals("NA")) {
						int depDelayTime = Integer.parseInt(colums[15]);
						if (depDelayTime > 0) {
							// ���Ű ����
							outputKey.set("D," + colums[0] + "," + colums[1]);
							// ��� ������ ����
							context.write(outputKey, outputValue);
						} else if (depDelayTime == 0) {
							context.getCounter(
									DelayCounters.scheduled_departure)
									.increment(1);
						} else if (depDelayTime < 0) {
							context.getCounter(DelayCounters.early_departure)
									.increment(1);
						}
					} else {
						context.getCounter(
								DelayCounters.not_available_departure)
								.increment(1);
					}

					// ���� ���� ������ ���
					if (!colums[14].equals("NA")) {
						int arrDelayTime = Integer.parseInt(colums[14]);
						if (arrDelayTime > 0) {
							// ���Ű ����
							outputKey.set("A," + colums[0] + "," + colums[1]);
							// ��� ������ ����
							context.write(outputKey, outputValue);
						} else if (arrDelayTime == 0) {
							context.getCounter(DelayCounters.scheduled_arrival)
									.increment(1);
						} else if (arrDelayTime < 0) {
							context.getCounter(DelayCounters.early_arrival)
									.increment(1);
						}
					} else {
						context.getCounter(DelayCounters.not_available_arrival)
								.increment(1);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
}
