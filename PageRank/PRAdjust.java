import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class PRAdjust {

    public static class Map extends Mapper<LongWritable, PRNodeWritable, LongWritable, PRNodeWritable>{
        private MultipleOutputs<LongWritable,DoubleWritable> output;

        public void setup(Context context) {
            output = new MultipleOutputs(context);
        }

        public void map(LongWritable key, PRNodeWritable value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", " ");
            String outDir = conf.get("outDir");
            double threshold = conf.getDouble("threshold", 0);
            double randomJumpFactor = conf.getDouble("randomJumpFactor", 0);
            boolean lastIteration = conf.getBoolean("lastIteration", false);
            long totalNodes = conf.getLong("totalNodes", 0);
            double missingMass = conf.getDouble("missingMass", 0);
    
            long adjustedPageRank = value.pageRank.get();
            adjustedPageRank += missingMass / totalNodes;
            adjustedPageRank *= 1 - randomJumpFactor;
            adjustedPageRank += PageRank.longConversion / totalNodes * randomJumpFactor;
            value.pageRank.set(adjustedPageRank);
            double outputPageRank = adjustedPageRank / PageRank.longConversion;
            context.write(key, value);
            if(outputPageRank >= threshold && lastIteration) {
                output.write("output", key, new DoubleWritable(outputPageRank), outDir+"/output");
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            output.close();
        }

    }

    public static class Reduce extends Reducer<LongWritable,PRNodeWritable,LongWritable,PRNodeWritable> {
    
    }

}