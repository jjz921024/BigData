package gdut.example.sql;

import gdut.utils.BaseDriver;
import gdut.utils.HdfsUtil;
import gdut.utils.JobInitModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 小表Join大表，将小表进行缓存
 * Created by Jun on 2018/3/15.
 */
public class MapJoin {

    public static class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {

        // 用于缓存小表的数据
        private Map<String, String> deptMap = new HashMap<String, String>();
        private String[] kv;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            BufferedReader in = null;
            try {
                // 从当前作业中获取要缓存的文件
                Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
                String deptIdName = null;
                for (Path path : paths) {
                    // 对部门文件字段进行拆分并缓存到deptMap中
                    if (path.toString().contains("departments")) {
                        in = new BufferedReader(new FileReader(path.toString()));
                        while (null != (deptIdName = in.readLine())) {
                            deptMap.put(deptIdName.split(",")[1], deptIdName.split(",")[0]);
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (in != null) {
                        in.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            kv = value.toString().split(",");
            // map join:在map阶段清洗数据，输出key为部门名称和value为部门no
            if (deptMap.containsKey(kv[1])) {
                if (null != kv[1] && !"".equals(kv[1])) {
                    context.write(new Text(deptMap.get(kv[1].trim())), new Text(kv[1].trim()));
                }
            }
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, IntWritable> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (Text val : values) {
                sum++;
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void run() throws InterruptedException, IOException, ClassNotFoundException {
        System.setProperty("HADOOP_USER_NAME", "jjz");
        Configuration conf = new Configuration();
        conf.set("mapreduce.job.jar", "D:/JavaWorkspace/CodeBase/classes/artifacts/JHadoop_jar/JHadoop.jar");

        String hdfs = HdfsUtil.getHdfsFromXML();
        String[] inPath = HdfsUtil.getPath("/user/jjz/dept_emp/part-m-*");
        String outPath = hdfs + "/user/jjz/result";

        JobInitModel job = new JobInitModel(inPath, outPath, conf, null, "MapJoin", MapJoin.class,
                null,
                JoinMapper.class, Text.class, Text.class,
                null, null,
                JoinReducer.class, Text.class, IntWritable.class);

        // 要缓存的文件路径
        DistributedCache.addCacheFile(new Path(inPath[1]).toUri(), conf);
        BaseDriver.initJob(new JobInitModel[]{job});
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        MapJoin.run();
    }
}
