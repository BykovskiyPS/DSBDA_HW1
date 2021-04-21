package bdtc.lab1;

import eu.bitwalker.useragentutils.Browser;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class HW1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

//    private final static IntWritable value = new IntWritable();
    private Text outputKey = new Text();

    // справочник метрик metricId - metricName
    private HashMap<String, String> metricDict = new HashMap<>();

    // значение масштаба временного окна
    private Integer scale;

    // значение масштаба временного окна в миллисекундах
    private Integer scaleInMillis;

    // первое значение времени в исходном файле
    private Long initialTime;

    // единица измерения масштаба временного окна (секунды, минуты, часы)
    private String unit;

    // шаблон строки из input файла
    private Pattern pattern = Pattern.compile("(\\d{1,2}), (\\d{13}), (\\d{1,5})");

    @Override
    protected void setup(Context context) {
        String metrics_line = context.getConfiguration().get("metrics_line");
        String[] inputArray = metrics_line.split(" ");
        initialTime = Long.parseLong(inputArray[0]);
        for(Integer i = 1; i < inputArray.length; i++) {
            String[] fields = inputArray[i].split("-");
            metricDict.put(fields[0], fields[1]);
        }

        unit = context.getConfiguration().get("time_unit");
        scale = Integer.parseInt(context.getConfiguration().get("time_window"));
        scaleInMillis = scale;
        switch (unit) {
            case ("s"):
                scaleInMillis *= 1000;
                break;
            case ("m"):
                scaleInMillis *= 60000;
                break;
            case ("h"):
                scaleInMillis *= 3600000;
                break;
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        Matcher match = pattern.matcher(line);
        if(match.find()) {
            String metricId = match.group(1);
            Long timeStamp = Long.parseLong(match.group(2));
            Integer inputValue = Integer.parseInt(match.group(3));
            outputKey.set(
                    metricDict.get(metricId) + ", " +
                    Long.toString(timeStamp - ((timeStamp - initialTime) % scaleInMillis)) + ", " +
                    scale + unit
            );
            context.write(outputKey, new IntWritable(inputValue));
        } else {
            context.getCounter(CounterType.MALFORMED).increment(1);
        }
    }
}
