import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class PageRank {

    public static final long longConversion = 100_000_000_000_000L;
    //enum type cannot be double
    public enum COUNTER {
        TOTAL_NODES,
        MISSING_MASS
    }

    public static class Map extends Mapper<LongWritable, PRNodeWritable, LongWritable, PRNodeWritable>{

        public void map(LongWritable key, PRNodeWritable value, Context context) throws IOException, InterruptedException {
            long totalNodes = context.getCounter(COUNTER.TOTAL_NODES).getValue();
            if (value.pageRank.get() < 0){
                value.pageRank.set(longConversion / totalNodes);
            }
            String[] neighbours = value.adjList.toStrings();
            long pageRankDistributed = value.pageRank.get() / neighbours.length;
            if(value.adjList.toStrings().length == 0) {
                context.getCounter(COUNTER.MISSING_MASS).increment(value.pageRank.get());
            }
            value.pageRank.set(0);
            context.write(key, value);
            for (String neighbour : neighbours){
                PRNodeWritable neighbourNode = new PRNodeWritable();
                neighbourNode.id.set(Long.parseLong(neighbour));
                neighbourNode.pageRank.set(pageRankDistributed);
                context.write(neighbourNode.id, neighbourNode);
            }
        }

    }

    public static class Reduce extends Reducer<LongWritable,PRNodeWritable,LongWritable,PRNodeWritable> {

        public void reduce(LongWritable key, Iterable<PRNodeWritable> values, Context context) throws IOException, InterruptedException {
            PRNodeWritable node = new PRNodeWritable();
            node.id = key;
            long pageRank = 0;
            for (PRNodeWritable nodeStruc : values){
                String[] adjList= nodeStruc.adjList.toStrings();
                if (adjList.length != 0){
                    Writable[] writableArr = PRPreProcess.convertToWritableArray(adjList);
                    node.adjList.set(writableArr);
                }
                pageRank += nodeStruc.pageRank.get();
            }
            node.pageRank.set(pageRank);
            context.write(key, node);
        }
    }

    public static void main(String[] args) throws Exception {
        FileSystem fs = FileSystem.get(new Configuration());
        Path outFilePath = new Path("/tmp/" + UUID.randomUUID().toString());
        Path finalOutFilePath = new Path(args[4]);
        double randomJumpFactor = Double.parseDouble(args[0]);
        int iteration = Integer.parseInt(args[1]);
        double threshold = Double.parseDouble(args[2]);
        Path inFilePath = new Path(args[3]);

        //pre-process
        Configuration preConf = new Configuration();
        preConf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", " ");

        Job preJob = Job.getInstance(preConf, "pre");
        preJob.setJarByClass(PRPreProcess.class);
        preJob.setInputFormatClass(KeyValueTextInputFormat.class);
        preJob.setOutputFormatClass(SequenceFileOutputFormat.class);

        preJob.setMapperClass(PRPreProcess.Map.class);
        preJob.setMapOutputKeyClass(LongWritable.class);
        preJob.setMapOutputValueClass(Text.class);

        preJob.setReducerClass(PRPreProcess.Reduce.class);
        preJob.setOutputKeyClass(LongWritable.class);
        preJob.setOutputValueClass(PRNodeWritable.class);

        FileInputFormat.addInputPath(preJob, inFilePath);
        FileOutputFormat.setOutputPath(preJob, outFilePath);
        System.exit(preJob.waitForCompletion(true) ? 0 : 1);

        //page rank

        long totalNodes = preJob.getCounters().findCounter(COUNTER.TOTAL_NODES).getValue();
        double missingMass = 0;

        for (int round = 1; round <= iteration; round++){

            inFilePath = outFilePath;
            outFilePath = new Path("/tmp/" + UUID.randomUUID().toString());
            Configuration pageRankConfig = new Configuration();
            pageRankConfig.set("mapreduce.output.textoutputformat.separator", " ");
            
            Job pageRankJob = Job.getInstance(pageRankConfig, "PageRank");
            pageRankJob.setJarByClass(PageRank.class);
            pageRankJob.setInputFormatClass(SequenceFileInputFormat.class);
            pageRankJob.setOutputFormatClass(SequenceFileOutputFormat.class);
            pageRankJob.setMapperClass(Map.class);
            pageRankJob.setMapOutputKeyClass(LongWritable.class);
            pageRankJob.setMapOutputValueClass(PRNodeWritable.class);
            pageRankJob.setReducerClass(Reduce.class);
            pageRankJob.setOutputKeyClass(LongWritable.class);
            pageRankJob.setOutputValueClass(PRNodeWritable.class);
            FileInputFormat.addInputPath(pageRankJob, inFilePath);
            FileOutputFormat.setOutputPath(pageRankJob, outFilePath);
            System.exit(pageRankJob.waitForCompletion(true) ? 0 : 1);

            //redistribute missing mass
            missingMass = pageRankJob.getCounters().findCounter(COUNTER.MISSING_MASS).getValue();
            inFilePath = outFilePath;
            outFilePath = new Path("/tmp/" + UUID.randomUUID().toString());

            boolean lastIteration = round == iteration;

            Configuration adjustConf = new Configuration();
            adjustConf.set("mapreduce.output.textoutputformat.separator", " ");
            adjustConf.set("outDir", finalOutFilePath.toString());
            adjustConf.setDouble("threshold", threshold);
            adjustConf.setDouble("randomJumpFactor", randomJumpFactor);
            adjustConf.setDouble("missingMass", missingMass);
            adjustConf.setLong("totalNodes", totalNodes);
            adjustConf.setBoolean("lastIteration", lastIteration);

            Job adjustJob = Job.getInstance(adjustConf, "adjust");
            adjustJob.setJarByClass(PRAdjust.class);
            adjustJob.setInputFormatClass(SequenceFileInputFormat.class);
            adjustJob.setOutputFormatClass(SequenceFileOutputFormat.class);

            adjustJob.setMapperClass(PRAdjust.Map.class);
            adjustJob.setMapOutputKeyClass(LongWritable.class);
            adjustJob.setMapOutputValueClass(PRNodeWritable.class);

            adjustJob.setReducerClass(PRAdjust.Reduce.class);
            adjustJob.setOutputKeyClass(LongWritable.class);
            adjustJob.setOutputValueClass(PRNodeWritable.class);
            
            MultipleOutputs.addNamedOutput(adjustJob, "text",
                TextOutputFormat.class,
                LongWritable.class,
                DoubleWritable.class);

            FileInputFormat.addInputPath(adjustJob, inFilePath);
            FileOutputFormat.setOutputPath(adjustJob, outFilePath);
            System.exit(adjustJob.waitForCompletion(true) ? 0 : 1);
        }
    }
}