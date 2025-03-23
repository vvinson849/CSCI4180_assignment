import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class MyDedup {
    
    static class ChunkAddress implements Serializable {
        int containerID;
        int offset;
        
        public ChunkAddress(int containerID, int offset) {
            this.containerID = containerID;
            this.offset = offset;
        }
    }
    
    static class ChunkMeta implements Serializable {
        int containerID;
        int offset;
        int chunkSize;
        String checksum;
        int referenceCount;
        
        public ChunkMeta(int containerID, int offset, int chunkSize, String checksum) {
            this.containerID = containerID;
            this.offset = offset;
            this.chunkSize = chunkSize;
            this.checksum = checksum;
            this.referenceCount = 1;
        }
    }
    
    
    static class IndexStruct implements Serializable {
        int numFiles;
        int numContainers;
        long numLogicalChunks;
        long numUniqueChunks;
        long numLogicalBytes;
        long numUniqueBytes;
        
        int latestContainerID;
        HashMap<String, ChunkMeta> indexMap; // <checksum, chunk>
        HashMap<Integer, Integer> containerUsage; // <containerID, referenceCount>
        
        public IndexStruct() {
            this.numFiles = 0;
            this.numContainers = 0;
            this.numLogicalChunks = 0L;
            this.numUniqueChunks = 0L;
            this.numLogicalBytes = 0L;
            this.numUniqueBytes = 0L;
            
            this.latestContainerID = 0;
            this.indexMap = new HashMap<String, ChunkMeta>();
            this.containerUsage = new HashMap<Integer, Integer>();
        }
        
        public void incrementReferenceCount(int containerID) {
            if (!containerUsage.containsKey(containerID))
                containerUsage.put(containerID, 0);
            int referenceCount = containerUsage.get(containerID);
            containerUsage.put(containerID, referenceCount+1);
        }
        
        public void decrementReferenceCount(int containerID) {
            if (!containerUsage.containsKey(containerID))
                return;
            int referenceCount = containerUsage.get(containerID);
            containerUsage.put(containerID, referenceCount-1);
        }
        
        public boolean emptyContainer(int containerID) {
            if (!containerUsage.containsKey(containerID))
                return true;
            return containerUsage.get(containerID) == 0;
        }
    }
    
    
    static IndexStruct loadIndex() throws IOException, ClassNotFoundException {
        IndexStruct index;
        File indexFile = new File("metadata/mydedup.index");
        if (!indexFile.exists()) {
            indexFile.getParentFile().mkdirs();
            indexFile.createNewFile();
            index = new IndexStruct();
        } else {
            FileInputStream indexFileLoader = new FileInputStream(indexFile);
            ObjectInputStream indexLoader = new ObjectInputStream(indexFileLoader);
            index = (IndexStruct) indexLoader.readObject();
            indexLoader.close();
            indexFileLoader.close();
        }
        return index;
    }
    
    
    static void storeIndex(IndexStruct index) throws FileNotFoundException, IOException {
        File indexFile = new File("metadata/mydedup.index");
        indexFile.delete();
        indexFile.createNewFile();
        FileOutputStream indexFileStorer = new FileOutputStream(indexFile);
        ObjectOutputStream indexStorer = new ObjectOutputStream(indexFileStorer);
        indexStorer.writeObject(index);
        indexStorer.close();
        indexFileStorer.close();
    }
    
    
    static class FileRecipeStruct implements Serializable {
        HashMap<String, List<ChunkAddress>> recipes; // <filename, checksums>
        
        public FileRecipeStruct() {
            this.recipes = new HashMap<String, List<ChunkAddress>>();
        }
    }
    
    
    static FileRecipeStruct loadFileRecipes() throws IOException, ClassNotFoundException {
        FileRecipeStruct fileRecipes;
        File fileRecipesFile = new File("metadata/file_recipes.index");
        if (!fileRecipesFile.exists()) {
            fileRecipesFile.getParentFile().mkdirs();
            fileRecipesFile.createNewFile();
            fileRecipes = new FileRecipeStruct();
        } else {
            FileInputStream fileRecipesFileLoader = new FileInputStream(fileRecipesFile);
            ObjectInputStream fileRecipesLoader = new ObjectInputStream(fileRecipesFileLoader);
            fileRecipes = (FileRecipeStruct) fileRecipesLoader.readObject();
            fileRecipesLoader.close();
            fileRecipesFileLoader.close();
        }
        return fileRecipes;
    }
    
    
    static void storeFileRecipes(FileRecipeStruct fileRecipes) throws FileNotFoundException, IOException {
        File fileRecipesFile = new File("metadata/file_recipes.index");
        fileRecipesFile.delete();
        fileRecipesFile.createNewFile();
        FileOutputStream fileRecipesFileStorer = new FileOutputStream(fileRecipesFile);
        ObjectOutputStream fileRecipeStorer = new ObjectOutputStream(fileRecipesFileStorer);
        fileRecipeStorer.writeObject(fileRecipes);
        fileRecipeStorer.close();
        fileRecipesFileStorer.close();
    }
    
    
    static String checksum(byte[] data) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(data, 0, data.length);
        byte[] checksumBytes = md.digest();
        return new String(checksumBytes);
    }
    
    
    static List<Integer> chunking(byte[] t, int m, int q, int max) {
        if ((m & 1) != 0 || (q & 1) != 0 || (max & 1) != 0) {
            System.err.println("parameters not power of 2");
            System.exit(1);
        }
        
        final BigInteger d = BigInteger.valueOf(257);
        final int mask = q-1;
        int[] rfp = new int[t.length-m];
        List<Integer> boundaries = new ArrayList<Integer>();
        boundaries.add(0);
        
        BigInteger dm_1 = d.pow(m-1);
        BigInteger ps = BigInteger.valueOf(0);
        BigInteger ps_1 = BigInteger.valueOf(0);
        for (int s=0; s<t.length-m; ++s) {
            if (s==0)
                for (int j=0; j<m; ++j)
                    ps = ps.add(d.pow(m-j-1).multiply(BigInteger.valueOf(t[j])));
            else                
                ps = d.multiply(ps_1.subtract(dm_1.multiply(BigInteger.valueOf(t[s-1])))).add(BigInteger.valueOf(t[s+m-1]));
            rfp[s] = ps.mod(BigInteger.valueOf(q)).intValue();
            ps_1 = ps;
        }
        for (int i=0; i<t.length-m; ++i) {            
            if (((rfp[i] & mask) == 0 && i + m - boundaries.get(boundaries.size()-1) >= m) || i + m - boundaries.get(boundaries.size()-1) == max)
                boundaries.add(i+m);
        }

        if (t.length - boundaries.get(boundaries.size()-1) > max)
            boundaries.add(boundaries.get(boundaries.size()-1) + m);
 
        return boundaries;
    }
    
    
    static void upload(int m, int q, int max, String filePath) throws IOException, ClassNotFoundException, NoSuchAlgorithmException {
        IndexStruct index = loadIndex();
        
        File f = new File(filePath);
        FileInputStream fileToUpload = new FileInputStream(f);
        byte[] fileData = new byte[(int)f.length()];
        fileToUpload.read(fileData);
        fileToUpload.close();

        List<Integer> boundaries = chunking(fileData, m, q, max);

        File containerFile = new File("data/"+(index.latestContainerID+1));
        if (!containerFile.exists()) {
            containerFile.getParentFile().mkdirs();
            containerFile.createNewFile();
        }
        
        FileOutputStream containerFileOutput = new FileOutputStream(containerFile);
        ByteArrayOutputStream containerBuffer = new ByteArrayOutputStream();
        List<ChunkAddress> chunkHashList = new ArrayList<String>();
        index.numLogicalChunks += boundaries.size();
        index.numLogicalBytes += fileData.length;
        
        boolean allChunksDuplicated = true;
        int numContainerBytes = 0;
        for (int i=0; i<boundaries.size(); ++i) {
            int start = boundaries.get(i);
            int end = i == boundaries.size()-1 ? fileData.length : boundaries.get(i+1);
            byte[] currChunk = Arrays.copyOfRange(fileData, start, end);
            String hashValue = checksum(currChunk);
            ChunkMeta currChunkMeta;

            if (index.indexMap.containsKey(hashValue)) {
                currChunkMeta = index.indexMap.get(hashValue);
                currChunkMeta.referenceCount++;
                chunkHashList.add(hashValue);
                index.incrementReferenceCount(currChunkMeta.containerID);
            } else {
                allChunksDuplicated = false;
                if (numContainerBytes + currChunk.length > 1048576) {
                    containerBuffer.writeTo(containerFileOutput);
                    containerBuffer.reset();
                    numContainerBytes = 0;
                    index.numContainers++;
                    index.latestContainerID++;
                    int containerID = index.latestContainerID + 1;
                    containerFile = new File("data/"+containerID);
                    containerFile.createNewFile();
                    containerFileOutput = new FileOutputStream(containerFile);
                }
                currChunkMeta = new ChunkMeta(index.latestContainerID+1, numContainerBytes, currChunk.length, hashValue);
                containerBuffer.write(currChunk);
                chunkHashList.add(hashValue);
                index.indexMap.put(hashValue, currChunkMeta);
                index.incrementReferenceCount(currChunkMeta.containerID);
                numContainerBytes += currChunk.length;
                index.numUniqueBytes += currChunk.length;
                index.numUniqueChunks++;
            }
            
            if (i == boundaries.size()-1) {
                containerBuffer.writeTo(containerFileOutput);
                index.numContainers++;
                index.latestContainerID++;
            }
        }
        containerFileOutput.close();
        containerBuffer.close();
        index.numFiles++;
        
        if(allChunksDuplicated) {
            containerFile.delete();
            index.numContainers--;
        }
        
        // write new mydedup.index
        storeIndex(index);
        
        // update file recipes
        FileRecipeStruct fileRecipes = loadFileRecipes();
        if (fileRecipes.recipes.containsKey(filePath))
            fileRecipes.recipes.remove(filePath);
        fileRecipes.recipes.put(filePath, chunkHashList);
        storeFileRecipes(fileRecipes);
        
        // report output
        System.out.println("Total number of files that have been stored: " + index.numFiles);
        System.out.println("Total number of pre-deduplicated chunks in storage: " + index.numLogicalChunks);
        System.out.println("Total number of unique chunks in storage: " + index.numUniqueChunks);
        System.out.println("Total number of bytes of pre-deduplicated chunks in storage: " + index.numLogicalBytes);
        System.out.println("Total number of bytes of unique chunks in storage: " + index.numUniqueBytes);
        System.out.println("Total number of containers in storage: " + index.numContainers);
        System.out.println("Deduplication ratio: " + String.format("%.2f", (float)index.numLogicalBytes/index.numUniqueBytes));        
    }
    
    
    static void download(String fileToDownload, String localFileName) throws IOException, ClassNotFoundException {
        IndexStruct index = loadIndex();
        
        FileRecipeStruct fileRecipes = loadFileRecipes();
        List<String> chunkHashList = null;
        try {
            chunkHashList = fileRecipes.recipes.get(fileToDownload);
        } catch (Exception e) {
            System.err.println(e);
            System.err.println("File Not Found");
            System.exit(1);
        }
        
        ByteArrayOutputStream chunkStorer = new ByteArrayOutputStream();
        for (int i=0; i<chunkHashList.size(); ++i) {
            String currChunkHash = chunkHashList.get(i);
            ChunkMeta currChunkMeta = index.indexMap.get(currChunkHash);
            FileInputStream containerLoader = new FileInputStream("data/"+currChunkMeta.containerID);
            containerLoader.skip(currChunkMeta.offset);
            byte[] chunk = new byte[currChunkMeta.chunkSize];
            containerLoader.read(chunk);
            chunkStorer.write(chunk);
            containerLoader.close();
        }
        
        File localFile = new File(localFileName);
        if (localFile.getParentFile() != null)
            localFile.getParentFile().mkdirs();
        if (!localFile.exists())
            localFile.createNewFile();
        FileOutputStream localFileStorer = new FileOutputStream(localFile);
        chunkStorer.writeTo(localFileStorer);
        chunkStorer.close();
        localFileStorer.close();
    }
    
    
    static void delete(String fileToDelete) throws IOException, ClassNotFoundException {
        // update file recipes
        FileRecipeStruct fileRecipes = loadFileRecipes();
        List<String> chunkHashList = null;
        try {
            chunkHashList = fileRecipes.recipes.get(fileToDelete);
        } catch (Exception e) {
            System.err.println(e);
            System.err.println("File Not Found");
            System.exit(1);
        }
        fileRecipes.recipes.remove(fileToDelete);
        storeFileRecipes(fileRecipes);
        
        // update mydedup.index
        IndexStruct index = loadIndex();
        for (String currChunkHash : chunkHashList) {
            ChunkMeta currChunkMeta = index.indexMap.get(currChunkHash);
            currChunkMeta.referenceCount--;
            // remove chunk from index
            index.numLogicalChunks--;
            index.numLogicalBytes -= currChunkMeta.chunkSize;
            index.decrementReferenceCount(currChunkMeta.containerID);
            // remove chunk if not used
            if (currChunkMeta.referenceCount == 0) {
                //System.out.println("cm.referenceCount == 0");
                index.indexMap.remove(currChunkMeta.checksum);
                index.numUniqueChunks--;
                index.numUniqueBytes -= currChunkMeta.chunkSize;
            }
            // remove container if all chunks are deleted
            if (index.emptyContainer(currChunkMeta.containerID)) {
                index.containerUsage.remove(currChunkMeta.containerID);
                index.numContainers--;
                File containerFile = new File("data/"+currChunkMeta.containerID);
                containerFile.delete();
            }
        }
        index.numFiles--;
        storeIndex(index);
    }

    
    public static void main(String[] args) throws FileNotFoundException, IOException, ClassNotFoundException, NoSuchAlgorithmException {
        
        switch (args[0]) {
            
            case "upload" :
                if (args.length != 5) {
                    System.err.println("parameters number mismatch");
                    System.exit(1);
                }
                final int m = Integer.parseInt(args[1]);
                final int q = Integer.parseInt(args[2]);
                final int max = Integer.parseInt(args[3]);
                final String filePath = args[4];               
                upload(m, q, max, filePath);                
                break;
          
            case "download" :
                if (args.length != 3) {
                    System.err.println("parameters number mismatch");
                    System.exit(1);
                }
                final String fileToDownload = args[1];
                final String localFileName = args[2];              
                download(fileToDownload, localFileName);               
                break;   
                
            case "delete" :
                if (args.length != 2) {
                    System.err.println("parameters number mismatch");
                    System.exit(1);
                }
                final String fileToDelete = args[1];
                delete(fileToDelete);
                break;

        }
        
    }
    
    
}
