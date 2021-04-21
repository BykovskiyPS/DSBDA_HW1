package bdtc.lab1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Редьюсер: суммирует все значения, полученные от {@link HW1Mapper}, выдаёт среднее значение метрики в агрегированном
 * интервале времени
 */
public class HW1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        int cnt = 0;
        while (values.iterator().hasNext()) {
            sum += values.iterator().next().get();
            cnt++;
        }
        context.write(key, new IntWritable(sum / cnt));
    }
}
