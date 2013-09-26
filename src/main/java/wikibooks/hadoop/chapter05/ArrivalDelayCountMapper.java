package wikibooks.hadoop.chapter05;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ArrivalDelayCountMapper extends
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
					// ���Ű ����
					outputKey.set(colums[0] + "," + colums[1]);
					if (!colums[14].equals("NA")) {
						int arrDelayTime = Integer.parseInt(colums[14]);
						if (arrDelayTime > 0) {
							// ��� ������ ����
							context.write(outputKey, outputValue);
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

	}
}
