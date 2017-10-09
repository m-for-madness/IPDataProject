package utilsClasses;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomValue implements WritableComparable<CustomValue> {
    private Long total;
    private Integer counter;

    public CustomValue() {
    }

    public CustomValue(long total, int counter) {
        this.total = total;
        this.counter = counter;
    }

    public Long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public Integer getCounter() {
        return counter;
    }

    public void setCounter(int counter) {
        this.counter = counter;
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
        return compare(total, o.total);

    }

    public static int compare(long k1, long k2) {
        return (k1 < k2 ? -1 : (k1 == k2 ? 0 : 1));
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        total = dataInput.readLong();
        counter = dataInput.readInt();

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(total);
        dataOutput.writeInt(counter);
    }
}
