package utilsClasses;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class KeyPartitioner extends Partitioner<CustomValue, Text> {

	@Override
	public int getPartition(CustomValue key, Text value, int numPartitions) {
		return (key.getTotal().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

}
