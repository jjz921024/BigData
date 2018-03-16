package gdut.example.workage;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Jun on 2018/2/24.
 */
public class WorkAgeReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int counter = 0;
        int result = 0;

        for (Text value : values) {
            result += Integer.parseInt(value.toString());
            counter++;
        }
        context.write(key, new Text(String.valueOf(result/counter)));

    }
}
