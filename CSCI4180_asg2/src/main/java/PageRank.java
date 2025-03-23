import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * Input: nodeID1 nodeID2 weight
 * Output: nodeID PRValue
 * iter: # of iterations
 * alpha: random jump factor
 * threshold: minimal PRValue for a node to outJob
 */

public class PageRank {
    
    public static class PRMapper extends Mapper<LongWritable, PRNodeWritable, LongWritable, PRNodeWritable> {
        
        private PRNodeWritable originNode;

        public void map(LongWritable nid, PRNodeWritable node, Context context)
                throws IOException, InterruptedException {
            
            originNode = new PRNodeWritable(nid.get(), 0);
            originNode.mergeAdjList(node.getAdjList());
            context.write(nid, originNode);
            
            double prValue = node.getPRval();
            if (prValue < 0) { // if not initialised
                Configuration conf = context.getConfiguration();
                final long N = Long.parseLong(conf.get("N"));
                prValue = 1.0 / (double)N;
            }

            double p = prValue / node.getAdjList().size();
            for (long m : node.getAdjList())
                context.write(new LongWritable(m), new PRNodeWritable(m, p));
            
        }
        
    }

    public static class PRReducer extends Reducer<LongWritable, PRNodeWritable, LongWritable, PRNodeWritable> {
        
        public static enum MCounter {M};
        
        public void reduce(LongWritable nid, Iterable<PRNodeWritable> nodes, Context context)
                throws IOException, InterruptedException {

            PRNodeWritable resultNode = new PRNodeWritable(nid.get(), 0);
            for (Writable n : nodes) {
                PRNodeWritable node = (PRNodeWritable) n;
                resultNode.addPRval(node.getPRval());
                resultNode.mergeAdjList(node.getAdjList());
            }
            long mbits = context.getCounter(MCounter.M).getValue();
            double m = Double.longBitsToDouble(mbits) + resultNode.getPRval();
            mbits = Double.doubleToLongBits(m);
            context.getCounter(MCounter.M).setValue(mbits);
            context.write(nid, resultNode);
            
        }
        
    }
    
    public static class OutputMapper extends Mapper<LongWritable, PRNodeWritable, LongWritable, PRNodeWritable> {

        public void map(LongWritable nid, PRNodeWritable node, Context context)
                throws IOException, InterruptedException {
            
            Configuration conf = context.getConfiguration();
            final double thre = Double.parseDouble(conf.get("thre"));
            double prValue = node.getPRval();
            if (prValue < 0) {
                final long N = Long.parseLong(conf.get("N"));
                prValue = 1.0 / (double)N;
                node = new PRNodeWritable(nid.get(), prValue);
            }
            if (prValue > thre)
                context.write(nid, node);
            
        }
        
    }

    public static class OutputReducer extends Reducer<LongWritable, PRNodeWritable, LongWritable, DoubleWritable> {
        
        public void reduce(LongWritable nid, Iterable<PRNodeWritable> nodes, Context context)
                throws IOException, InterruptedException {
            
            for (Writable w : nodes) {
                PRNodeWritable n = (PRNodeWritable) w;
                context.write(nid, new DoubleWritable(n.getPRval()));
            }
            
        }
        
    }

    public static void main(String[] args) throws Exception {
        
        if (args.length < 5) {
            System.err.println("Usage: PageRank <alpha> <iteration> <threshold> <infile> <outdir>");
            System.exit(-1);
        }
        
        final int iter = Integer.parseInt(args[1]);
        
        // Launch a MapRed job for pre-processing module
        Configuration confpre = new Configuration();
        Job preProcessJob = Job.getInstance(confpre, "pre-processing");
        preProcessJob.setJarByClass(PRPreProcess.class);
        preProcessJob.setMapperClass(PRPreProcess.PreProcessMapper.class);
        preProcessJob.setReducerClass(PRPreProcess.PreProcessReducer.class);
        preProcessJob.setMapOutputKeyClass(LongWritable.class);
        preProcessJob.setMapOutputValueClass(LongWritable.class);
        preProcessJob.setOutputKeyClass(LongWritable.class);
        preProcessJob.setOutputValueClass(PRNodeWritable.class);
        preProcessJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(preProcessJob, new Path(args[3]));
        FileOutputFormat.setOutputPath(preProcessJob, new Path(args[3]+"/0b"));
        preProcessJob.waitForCompletion(true);
        final long N = preProcessJob.getCounters().findCounter(PRPreProcess.NodeCounter.N).getValue();
        
        
        // Main loop to calculate per-node PageRank value iteratively
        for (int i=1; i<=iter; ++i) {
            // job a
            Configuration confpr = new Configuration();
            confpr.set("N", Long.toString(N));
            Job prJob = Job.getInstance(confpr);
            prJob.setJarByClass(PageRank.class);
            prJob.setMapperClass(PRMapper.class);
            prJob.setReducerClass(PRReducer.class);
            prJob.setOutputKeyClass(LongWritable.class);
            prJob.setOutputValueClass(PRNodeWritable.class);
            prJob.setInputFormatClass(SequenceFileInputFormat.class);
            prJob.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileInputFormat.addInputPath(prJob, new Path(args[3]+"/"+(i-1)+"b/part-r-00000"));
            FileOutputFormat.setOutputPath(prJob, new Path(args[3]+"/"+i+"a"));
            prJob.waitForCompletion(true);
            long mbits = prJob.getCounters().findCounter(PRReducer.MCounter.M).getValue();
            // job b
            Configuration confadj = new Configuration();
            confadj.set("alpha", args[0]);
            confadj.set("m", Long.toString(mbits));
            confadj.set("N", Long.toString(N));
            Job adjJob = Job.getInstance(confadj);
            adjJob.setJarByClass(PRAdjust.class);
            adjJob.setMapperClass(PRAdjust.AdjustMapper.class);
            adjJob.setReducerClass(PRAdjust.AdjustReducer.class);
            adjJob.setOutputKeyClass(LongWritable.class);
            adjJob.setOutputValueClass(PRNodeWritable.class);
            adjJob.setInputFormatClass(SequenceFileInputFormat.class);
            adjJob.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileInputFormat.addInputPath(adjJob, new Path(args[3]+"/"+i+"a/part-r-00000"));
            FileOutputFormat.setOutputPath(adjJob, new Path(args[3]+"/"+i+"b"));
            adjJob.waitForCompletion(true);
        }
        
        
        // Cleanup for requirement outJob format
        Configuration confout = new Configuration();
        confout.set("thre", args[2]);
        confout.set("N", Long.toString(N));
        Job outJob = Job.getInstance(confout);
        outJob.setJarByClass(PageRank.class);
        outJob.setMapperClass(OutputMapper.class);
        outJob.setReducerClass(OutputReducer.class);
        outJob.setOutputKeyClass(LongWritable.class);
        outJob.setOutputValueClass(PRNodeWritable.class);    
        outJob.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(outJob, new Path(args[3]+"/"+iter+"b/part-r-00000"));
        FileOutputFormat.setOutputPath(outJob, new Path(args[4]));
        System.exit(outJob.waitForCompletion(true) ? 0 : 1);
        
    }
    
}
