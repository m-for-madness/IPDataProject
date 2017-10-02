package utilsClasses;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomValue  implements WritableComparable<CustomValue> {
    private LongWritable total;
    private IntWritable counter;

    public CustomValue() {
    }

    public CustomValue(long total, int counter) {
        this.total = new LongWritable(total);
        this.counter = new IntWritable(counter);
    }

    public long getTotal() {
        return total.get();
    }

    public void setTotal(long total) {
        this.total.set(total);
    }

    public int getCounter() {
        return counter.get();
    }

    public void setCounter(int counter) {
        this.counter.set(counter);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CustomValue that = (CustomValue) o;

        if (!total.equals(that.total)) return false;
        return counter.equals(that.counter);
    }

    @Override
    public int hashCode() {
        int result = total.hashCode();
        result = 31 * result + counter.hashCode();
        return result;
    }

    @Override
    public int compareTo(CustomValue o) {
        return total.compareTo(o.total);

    }


    @Override
    public void readFields(DataInput dataInput) throws IOException {
        total =  new LongWritable(dataInput.readLong());
        counter = new IntWritable(dataInput.readInt());

    }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(total.get());
        dataOutput.writeInt(counter.get());
    }
}
