import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class PRAdjust {
    
    public static class AdjustMapper
        extends Mapper<LongWritable, PRNodeWritable, LongWritable, PRNodeWritable> {

        public void map(LongWritable nid, PRNodeWritable node, Context context)
                throws IOException, InterruptedException {
            
            Configuration conf = context.getConfiguration();
            double alpha = Double.parseDouble(conf.get("alpha"));
            long mbits = Long.parseLong(conf.get("m"));
            double m = 1 - Double.longBitsToDouble(mbits);
            long N = Long.parseLong(conf.get("N"));
            node.adjustPRval(alpha, m, N);
            context.write(nid, node);
            
        }
        
    }

    public static class AdjustReducer
        extends Reducer<LongWritable, PRNodeWritable, LongWritable, PRNodeWritable> {

        public void reduce(LongWritable nid, Iterable<PRNodeWritable> nodes, Context context)
                throws IOException, InterruptedException {
            
            for (Writable w : nodes) {
                PRNodeWritable n = (PRNodeWritable) w;
                context.write(nid, n);
            }
            
        }

    }
    
}
