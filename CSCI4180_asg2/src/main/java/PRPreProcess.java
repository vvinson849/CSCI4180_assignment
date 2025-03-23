import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Input: <node ID1> <node ID 2> <weight>
 * Output: <node ID> <node>
 */

public class PRPreProcess {
    
    public static enum NodeCounter {N};
    
    public static class PreProcessMapper
        extends Mapper<Object, Text, LongWritable, LongWritable> {

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            
            String[] input = value.toString()
                                .trim()
                                .split(" ");
            long id = Long.parseLong(input[0]);
            long neighbour = Long.parseLong(input[1]);
            context.write(new LongWritable(id), new LongWritable(neighbour));
            context.write(new LongWritable(neighbour), new LongWritable(neighbour));
            
        }
        
    }

    public static class PreProcessReducer
        extends Reducer<LongWritable, LongWritable, LongWritable, PRNodeWritable> {
        
        private PRNodeWritable outputNode;

        public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            
            outputNode = new PRNodeWritable(key.get(), -1);
            
            for (Writable n : values) {
                LongWritable neighbour = (LongWritable) n;
                outputNode.addNeighbour(neighbour.get());
            }
            
            context.getCounter(NodeCounter.N).increment(1);
            context.write(key, outputNode);
            
        }

    }
    
    public static void main(String[] args) throws Exception {
        
        Configuration conf = new Configuration();
        
        // for testing pre-processing module
        Job preProcessJob = Job.getInstance(conf, "pre-processing");
        preProcessJob.setJarByClass(PRPreProcess.class);
        preProcessJob.setMapperClass(PRPreProcess.PreProcessMapper.class);
        preProcessJob.setReducerClass(PRPreProcess.PreProcessReducer.class);
        preProcessJob.setOutputKeyClass(LongWritable.class);
        preProcessJob.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(preProcessJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(preProcessJob, new Path(args[1]));
        System.exit(preProcessJob.waitForCompletion(true) ? 0 : 1);
        
    }
    
}
