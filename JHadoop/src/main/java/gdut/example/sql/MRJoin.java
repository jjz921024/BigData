package gdut.example.sql;

import gdut.utils.BaseDriver;
import gdut.utils.JobInitModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.HashSet;

/**
 * 两个表join操作
 * Created by Jun on 2018/2/28.
 */
public class MRJoin {

    public static class JoinMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        IntWritable k = new IntWritable();
        Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 获取分片的路径
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            String[] split = value.toString().split(",");
            // employees表join salaries表
            if (fileName.contains("employees")) {
                //将join on字段作为id
                k.set(Integer.parseInt(split[0]));
                //查询的字段作为value, 加上标识符表示哪个表来的记录
                v.set(new Text("0-" + split[2] + split[3]));

            } else if (fileName.contains("salaries")) {
                k.set(Integer.parseInt(split[0]));
                v.set(new Text("1-" + split[1]));
            }
            context.write(k, v);
        }
    }

    public static class JoinReduce extends Reducer<IntWritable, Text, Text, Text> {
        Text k = new Text();
        Text v = new Text();

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String temp = "";
            HashSet<String> set = new HashSet<>();
            for (Text value : values) {
                // left join 以左表为key
                // 假定是以主键作为join的字段
                if (value.toString().startsWith("0-")) {
                    temp = value.toString().split("-")[1];
                }
                if (value.toString().startsWith("1-")) {
                    set.add(value.toString().split("-")[1]);
                }
            }

            k.set(temp);
            for (String s:set) {
                v.set(s);
                context.write(k, v);
            }
        }
    }

    public static void run() throws InterruptedException, IOException, ClassNotFoundException {
        System.setProperty("HADOOP_USER_NAME", "jjz");
        Configuration conf = new Configuration();
        conf.set("mapreduce.job.jar", "D:/JavaProjects/CodeBase/out/artifacts/JHadoop_jar/JHadoop.jar");

        String hdfs = "hdfs://119.23.43.240:9000";
        String[] inPath = new String[]{hdfs + "/user/jjz/salaries/part-*",
                hdfs + "/user/jjz/employees/part-m-00000",
                hdfs + "/user/jjz/employees/part-m-00001",
                hdfs + "/user/jjz/employees/part-m-00002",
                hdfs + "/user/jjz/employees/part-m-00003"};
        String outPath = hdfs + "/user/jjz/result";

        JobInitModel job = new JobInitModel(inPath, outPath, conf, null, "MRJoin", MRJoin.class,
                null,
                MRJoin.JoinMapper.class, IntWritable.class, Text.class,
                null, null,
                MRJoin.JoinReduce.class, Text.class, Text.class);
        BaseDriver.initJob(new JobInitModel[]{job});
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        MRJoin.run();
    }
}
