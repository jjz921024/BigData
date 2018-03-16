package gdut.example.workage;


import gdut.utils.BaseDriver;
import gdut.utils.HdfsUtil;
import gdut.utils.JobInitModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * 按性别统计员工入职的平均年龄
 * Created by Jun on 2018/2/24.
 */
public class WorkAge {

    public static void run() throws InterruptedException, IOException, ClassNotFoundException {
        //设置user 或 hadoop fs -chmod -R 777 $HADOOP_HOME调整权限
        System.setProperty("HADOOP_USER_NAME", "jjz");

        Configuration conf = new Configuration();
        // 客户端提交的jar到集群   设置编译后自动打包
        // 注意：集群上HADOOP_CLASSPATH路径下不要有该jar包，否则会覆盖提交的新jar包
        conf.set("mapreduce.job.jar", "D:/JavaWorkspace/CodeBase/classes/artifacts/JHadoop_jar/JHadoop.jar");

        String hdfs = HdfsUtil.getHdfsFromXML();
        String[] inPath = HdfsUtil.getPath("/user/jjz/employees/part-m-*");
        String outPath = hdfs + "/user/jjz/result";

        JobInitModel job = new JobInitModel(inPath, outPath, conf, null, "WorkAgeByGender", WorkAge.class,
                null,
                WorkAgeMapper.class, Text.class, Text.class,
                null, null,
                WorkAgeReducer.class, Text.class, Text.class);
        BaseDriver.initJob(new JobInitModel[]{job});
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        WorkAge.run();
    }
}
