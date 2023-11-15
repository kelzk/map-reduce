import java.io.IOException;
import java.lang.StringBuilder;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class PRPreProcess {

    public static Writable[] convertToWritableArray(String[] stringArray) {
        Writable[] writableArray = new Writable[stringArray.length];

        for (int i = 0; i < stringArray.length; i++) {
            writableArray[i] = new Text(stringArray[i]);
        }

        return writableArray;
    }

    public static class Map
        extends Mapper<Text,Text,LongWritable,Text> {

            public void map(Text key, Text val, Context context) throws IOException, InterruptedException {
            
                String[] nodes = val.toString().split(" ");
                context.write(new LongWritable(Long.parseLong(nodes[0])), new Text(nodes[1]));
          
            }
    }

    public static class Reduce
        extends Reducer<LongWritable,Text,LongWritable,PRNodeWritable> {

            public void reduce(LongWritable key, Iterable<Text> val, Context context) throws IOException, InterruptedException {

                PRNodeWritable node = new PRNodeWritable();
                node.id = key;
                StringBuilder sb = new StringBuilder();
                for(Text nodeId : val) {
                    sb.append(nodeId.toString()+" ");
                }
                String[] strArr = sb.toString().split(" ");
                Writable[] writableArr = convertToWritableArray(strArr);
                node.adjList.set(writableArr);
                context.write(node.id, node);
                context.getCounter(PageRank.COUNTER.TOTAL_NODES).increment(1);
            }
    }

}