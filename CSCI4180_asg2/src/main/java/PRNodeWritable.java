import java.util.List;
import java.util.ArrayList;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class PRNodeWritable implements Writable {
        
    private long id;
    private double prValue;
    private List<Long> adjList = new ArrayList<>();
    
    public PRNodeWritable() {
        this.id = -1;
        this.prValue = -1;
    }

    public PRNodeWritable(long id, double prValue) {
        this.id = id;
        this.prValue = prValue;
    }
    
    public long getID() {
        return id;
    }
    
    public double getPRval() {
        return prValue;
    }
    
    public List<Long> getAdjList() {
        return adjList;
    }
    
    public void addPRval(double prValue) {
        this.prValue += prValue;
    }
    
    public void adjustPRval(double alpha, double m, long N) {
        prValue = alpha/N + (1-alpha)*(m/N + prValue);
    }
    
    public void addNeighbour(long id) {
        if (this.id != id)
            adjList.add(id);
    }
    
    public void mergeAdjList(List<Long> list) {
        for (long id : list)
            addNeighbour(id);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(id);
        out.writeDouble(prValue);
        out.writeLong(adjList.size());
        for (Long neighbour : adjList) {
            out.writeLong(neighbour);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readLong();
        prValue = in.readDouble();
        long size = in.readLong();
        adjList.clear();
        for (long i = 0; i < size; i++) {
            adjList.add(in.readLong());
        }
        
    }

}
