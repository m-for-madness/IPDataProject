package reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utilsClasses.CustomValue;

import java.io.IOException;
import java.util.Iterator;

public class IPDataReducer extends Reducer<Text, CustomValue, Text, Text> {

    public void reduce(Text key, Iterable<CustomValue> values, Reducer<Text, CustomValue, Text, Text>.Context con) throws IOException, InterruptedException {
        Iterator<CustomValue> itr = values.iterator();
        DoubleWritable average;
        long sum = 0;
        Integer counter = 0;
        CustomValue value = new CustomValue();
        while(itr.hasNext()){
            value = itr.next();
            sum+=value.getTotal();
            counter+=value.getCounter();
        }
        average = new DoubleWritable(sum/((double)counter));
        con.write(key, new Text(average+ ","+sum));

    }
}
