package gdut.example.workage;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by Jun on 2018/2/24.
 */
public class WorkAgeMapper extends Mapper<LongWritable, Text, Text, Text> {

    private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private Calendar calendar = Calendar.getInstance();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split(",");

        try {
            String gender = split[4];
            Date birthDate = simpleDateFormat.parse(split[1]);   //出生日期小于1970
            Date workDate = simpleDateFormat.parse(split[5]);

            calendar.setTime(birthDate);
            int birthYear = calendar.get(Calendar.YEAR);
            calendar.setTime(workDate);
            int workYear = calendar.get(Calendar.YEAR);

            context.write(new Text(gender), new Text(Integer.toString(workYear - birthYear)));
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
