package combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utilsClasses.CustomValue;

import java.io.IOException;
import java.util.Iterator;

public class IPDataCombiner  extends Reducer<Text, CustomValue, Text, CustomValue> {
    private long sum;
    private CustomValue customValue;
    private Integer counter;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        sum = 0;
        customValue = new CustomValue();
        counter = 1;
    }

    @Override
    protected void reduce(Text key, Iterable<CustomValue> values, Reducer<Text, CustomValue, Text, CustomValue>.Context context) throws IOException, InterruptedException {
        sum = 0;
        counter = 1;
        Iterator<CustomValue> itr = values.iterator();
        while(itr.hasNext()){
            customValue = itr.next();
            sum+=customValue.getTotal();
            counter+=customValue.getCounter();
        }
        customValue.setCounter(counter);
        customValue.setTotal(sum);
        context.write(key, customValue);
    }
}
