import java.io.IOException;
import java.util.StringTokenizer;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NgramInitialRF2 {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, MapWritable>{
        LinkedList<String> list;
        Map<String, Map<String, Integer>> map;

        public void setup(Context context){
            list = new LinkedList<String>();
            map = new HashMap<String, Map<String, Integer>>();
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            int N = context.getConfiguration().getInt("N", 2);
            if (N < 1) {
                System.exit(0);
            }

            StringTokenizer itr = new StringTokenizer(value.toString(), " \t\n\r\f.,:;?![]'\"(){}<>-+*/\\=|%`~@#$^&_0123456789‘’“”");
            
            while (itr.hasMoreTokens()) {
                String initial = String.valueOf(itr.nextToken().charAt(0));
                list.addLast(initial);

                String firstToken = list.getFirst();
                if (list.size() == N && map.containsKey(firstToken)){
                    list.removeFirst();
                    String ngram = String.join(" ", list);
                    Map<String, Integer> innerMap = map.get(firstToken);
                    innerMap.put(ngram, innerMap.containsKey(ngram)? innerMap.get(ngram) + 1 : 1);
                }
                if (list.size() == N && !(map.containsKey(firstToken))){
                    list.removeFirst();
                    String ngram = String.join(" ", list);
                    Map<String, Integer> innerMap = new HashMap<String, Integer>();
                    innerMap.put(ngram, 1);
                    map.put(firstToken, innerMap);
                }

            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, Map<String, Integer>> entry : map.entrySet()) {
                
                MapWritable mapWritable = new MapWritable();
                for (Map.Entry<String, Integer> innerEntry : entry.getValue().entrySet()) {
                    mapWritable.put(new Text(innerEntry.getKey()), new IntWritable(innerEntry.getValue()));
                }
                context.write(new Text(entry.getKey()), mapWritable);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text,MapWritable,Text,Text> {

        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            double THETA = context.getConfiguration().getDouble("THETA", 0);

            Map<Text,IntWritable> result = new HashMap<Text,IntWritable>();
            for (MapWritable stripe : values){
                for (Map.Entry<Writable, Writable> entry : stripe.entrySet()){
                    if (result.containsKey((Text) entry.getKey())){
                        int newVal = result.get(entry.getKey()).get() + ((IntWritable) (entry.getValue())).get();
                        result.put((Text) entry.getKey(), new IntWritable(newVal));
                    }
                    if (!(result.containsKey((Text) entry.getKey()))){
                        result.put((Text) entry.getKey(), (IntWritable) entry.getValue());
                    }
                }
            }
            
            int total = 0;
            
            for (Map.Entry<Text,IntWritable> entry : result.entrySet()){
                total += entry.getValue().get();
            }

            for (Map.Entry<Text,IntWritable> entry : result.entrySet()){
                double rf = (double)(entry.getValue().get()) / (double)total;
                if (String.valueOf(entry.getKey()).equals("") && rf >= THETA ){
                    context.write(key, new Text(String.valueOf(entry.getKey()) + String.valueOf(rf)));
                }
                if (!String.valueOf(entry.getKey()).equals("") && rf >= THETA){
                    context.write(key, new Text(String.valueOf(entry.getKey()) + " " + String.valueOf(rf)));
                }
              
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", " ");
        conf.setInt("N", Integer.parseInt(args[2]));
        conf.setDouble("THETA", Double.parseDouble(args[3]));
        Job job = Job.getInstance(conf, "NgramInitialRF");
        job.setJarByClass(NgramInitialRF2.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}