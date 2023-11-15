import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.ArrayWritable;

public class PRNodeWritable implements Writable {
  public LongWritable id;
  public LongWritable pageRank;
  public ArrayWritable adjList;

  public PRNodeWritable() {
    String[] strArr = {};
    id = new LongWritable();
    pageRank = new LongWritable(-1);
    adjList = new ArrayWritable(strArr);
  }

  public void write(DataOutput out) throws IOException {
    id.write(out);
    pageRank.write(out);
    adjList.write(out);
  }

  public void readFields(DataInput in) throws IOException {
    id.readFields(in);
    pageRank.readFields(in);
    adjList.readFields(in);
  }
}