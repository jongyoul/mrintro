package wikibooks.hadoop.chapter05;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.ChainReducer;

import java.io.IOException;

public class ChainingExample {
    public static void main(String[] args) throws IOException {
        JobConf conf = new JobConf();

        //MapReduce job 이름 설정
        conf.setJobName("Chain");
        //입출력 데이터 포맷 설정
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        //첫 번째 Mapper 설정
        JobConf mapAConf = new JobConf(false);
        ChainMapper.addMapper(conf, AMap.class, LongWritable.class, Text.class, Text.class, Text.class, true, mapAConf);

        //두 번째 Mapper 설정
        JobConf mapBConf = new JobConf(false);
        ChainMapper.addMapper(conf, BMap.class, Text.class, Text.class, LongWritable.class, Text.class, false, mapBConf);

        //Reducer 설정
        JobConf reduceConf = new JobConf(false);
        ChainReducer.setReducer(conf, XReduce.class, LongWritable.class, Text.class, Text.class, Text.class, true, reduceConf);

        //세 번째 Mapper 설정(리듀서 실행 후 실행됨)
        ChainReducer.addMapper(conf, CMap.class, Text.class, Text.class, LongWritable.class, Text.class, false, null);

        //네 번째 Mapper 설정
        ChainReducer.addMapper(conf, DMap.class, LongWritable.class, Text.class, LongWritable.class, LongWritable.class, true, null);

        FileInputFormat.setInputPaths(conf, inDir);
        FileOutputFormat.setOutputPath(conf, outDir);

        JobClient jc = new JobClient(conf);
        RunningJob job = jc.submitJob(conf);

    }
}
