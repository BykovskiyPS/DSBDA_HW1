package bdtc.lab1;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;


@Log4j
public class MapReduceApplication {

    public static void main(String[] args) throws Exception {
        /*
            Входный аргументы args:
            args[0] - входной файл с данными
            args[1] - выходной агрегированный файл
            args[2] - путь до файла справочника обозначений
            args[3] - ширина временного интервала
            args[4] - единица измерения временного интервала
                     (s - секунды, m - минуты, h - часы)
         */
        if (args.length < 5) {
            throw new RuntimeException(
                    "You should specify input and output folders,\n" +
                    "metric dictionary, time window, time unit (s - second, m - minute, h - hour)"
            );
        }
        if (!args[4].equals("s") && !args[4].equals("m") && !args[4].equals("h")) {
            throw new RuntimeException("Wrong time unit. Enter s, m or h");
        }
        Configuration conf = new Configuration();
        conf.set("time_window", args[3]);
        conf.set("time_unit", args[4]);

        String line = "";
        try {
            File file = new File(args[2]);
            FileReader fr = new FileReader(file);
            BufferedReader reader = new BufferedReader(fr);
            line = reader.readLine();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        conf.set("metrics_line", line);

        // задаём выходной файл, разделенный запятыми - формат CSV в соответствии с заданием
        //  conf.set("mapreduce.output.textoutputformat.separator", ",");

        Job job = Job.getInstance(conf, "raw metrics");
        job.setJarByClass(MapReduceApplication.class);
        job.setMapperClass(HW1Mapper.class);
        job.setReducerClass(HW1Reducer.class);
        // Использование Combiner в соответствии с заданием
        job.setCombinerClass(HW1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        Path outputDirectory = new Path(args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputDirectory);
        log.info("=====================JOB STARTED=====================");
        job.waitForCompletion(true);
        log.info("=====================JOB ENDED=====================");
        // проверяем статистику по счётчикам
        Counter counter = job.getCounters().findCounter(CounterType.MALFORMED);
        log.info("=====================COUNTERS " + counter.getName() + ": " + counter.getValue() + "=====================");
    }
}
