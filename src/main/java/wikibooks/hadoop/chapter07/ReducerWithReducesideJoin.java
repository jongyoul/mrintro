package wikibooks.hadoop.chapter07;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerWithReducesideJoin extends Reducer<Text, Text, Text, Text> {

	// reduce ���Ű
	private Text outputKey = new Text();
	private Text outputValue = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// �±� ��ȸ
		String tagValue = key.toString().split("_")[1];

		for (Text value : values) {
			// ���Ű ����
			if (tagValue.equals(CarrierCodeMapper.DATA_TAG)) {
				outputKey.set(value);
				// ��°� ���� �� ��� ������ ����
			} else if (tagValue.equals(MapperWithReducesideJoin.DATA_TAG)) {
				outputValue.set(value);
				context.write(outputKey, outputValue);
			}

		}

	}
}
