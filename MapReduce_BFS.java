import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileReader;
import java.io.FileWriter;
import java.lang.Integer;
import java.io.IOException;
import java.lang.Math;
import java.util.*;
import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class SmallWorld {
    // Maximum dept for any breadth-first search
    public static final int MAX_ITERATIONS = 20;
    public static boolean isDone = false;
    public static final int UNCHECKED_DISTANCE = -1;
    public static final int UNCHECKED = -1;
    public static final int CHECKED = 0;
    public static final int FINISHED = 1;
    public static final int VIR = 999999;

    static void delete(File f) throws IOException {
	if (f.isDirectory()) {
	  for (File c : f.listFiles())
	    delete(c);
	}
	if (!f.delete())
	  System.out.println("Didn't delete");
    }


    // Skeleton code uses this to share denom cmd-line arg across cluster
    public static final String DENOM_PATH = "denom.txt";

    // Example enumerated type, used by EValue and Counter example
    public static enum ValueUse {EDGE};

    // Example writable type
    public static class EValue implements Writable {
        public ValueUse use;
        public long value;

        public EValue(ValueUse use, long value) {
            this.use = use;
            this.value = value;
        }

        public EValue() {
            this(ValueUse.EDGE, 0);
        }

        // Serializes object - needed for Writable
        public void write(DataOutput out) throws IOException {
            out.writeUTF(use.name());
            out.writeLong(value);
        }

        // Deserializes object - needed for Writable
        public void readFields(DataInput in) throws IOException {
            use = ValueUse.valueOf(in.readUTF());
            value = in.readLong();
        }

        public void set(ValueUse use, long value) {
            this.use = use;
            this.value = value;
        }

        public String toString() {
            return use.name() + ": " + value;
        }
    }


    /* This example mapper loads in all edges but only propagates a subset.
       You will need to modify this to propagate all edges, but it is 
       included to demonstate how to read & use the denom argument.         */
    public static class LoaderMap extends Mapper<LongWritable, LongWritable, LongWritable, Text> {
        public long denom;

        /* Setup is called automatically once per map task. This will
           read denom in from the DistributedCache, and it will be
           available to each call of map later on via the instance
           variable.                                                  */
        @Override
        public void setup(Context context) {
            try {
                Configuration conf = context.getConfiguration();
                Path cachedDenomPath = DistributedCache.getLocalCacheFiles(conf)[0];
                BufferedReader reader = new BufferedReader(
                                        new FileReader(cachedDenomPath.toString()));
                String denomStr = reader.readLine();
                reader.close();
                denom = Long.decode(denomStr);
            } catch (IOException ioe) {
                System.err.println("IOException reading denom from distributed cache");
                System.err.println(ioe.toString());
            }
        }

        /* Will need to modify to not lose any edges. */
        @Override
        public void map(LongWritable key, LongWritable value, Context context)
                throws IOException, InterruptedException {
            // Send edge forward only if part of random subset
    	    Text temp;
            if (Math.random() < 1.0/denom) {					// if chosen as a start node
		temp = new Text (new String("s" + value.toString()));		// add an s to mark it for later
            } else {
		temp = new Text (new String(value.toString()));
	    }
	    context.write(key, temp);
            // Example of using a counter (counter tagged by EDGE)
            context.getCounter(ValueUse.EDGE).increment(1);
        }
    }

    public static class GrapherReduce extends Reducer<LongWritable, Text, LongWritable, Text> {
	public void reduce(LongWritable key, Iterable<Text> values, Context context)
	    throws IOException, InterruptedException {
	    String children = new String();
	    int startTag = UNCHECKED_DISTANCE;
	    int startCheck = UNCHECKED;
	    String valueString;
	    int source = -1;
	    for(Text value : values) {
		valueString = value.toString();
		if (valueString.contains("s")) {				// if it is a start node
		    valueString = valueString.substring(1);			// remove the s and set startTag to represent a distance of 0 away
		    startTag = 0;
		    startCheck = CHECKED;
		    source = Integer.parseInt(key.toString());
		    children += valueString + ",";
		} else {
		    children += value.toString() + ",";
		}
	    }
	    children = children.substring(0, children.length()-1); 		// cuts off last character of children
	    String temp = new String(startTag + "|" + children + "|" + startCheck + "|" + source);
	    Text emitVal = new Text(temp);
	    context.write(key, emitVal);
	    Text dummyVal = new Text(new String("#|" + children + "|#|#"));
	    context.write(key, dummyVal);
	}
    }

    public static class BFSMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	public void map(LongWritable key, Text value, Context context)
	    throws IOException, InterruptedException {
	    String[] keyArray 		= value.toString().split("\t");
	    LongWritable finalKey 	= new LongWritable(Long.parseLong(keyArray[0]));
	    String[] temp 		= value.toString().split("\\|");
	    String[] distanceArray  	= temp[0].split("\t");
	    String distance 		= distanceArray[1];
	    String children  		= temp[1];
	    String status	    	= temp[2];
	    String source    		= temp[3];

	    // if checked, emit finished version and checked children
	    if (!status.equals("#")) {
	    	if (Integer.parseInt(status)==CHECKED) {
	     	    Text emitText = new Text(new String( distance + "|" + children + "|" + FINISHED + "|" + source));
	   	    context.write(finalKey, emitText);
		    String[] point = children.split(",");
		    for (int i = 0; i < point.length; i++) {
		   	long newLongKey = Long.parseLong(point[i]);
		    	LongWritable newKey = new LongWritable(newLongKey);

		    	String newDistance = Integer.toString(((Integer.parseInt(distance)) + 1));
		    	Text mapKV = new Text(new String(newDistance + "|" + VIR + "|" + CHECKED + "|" + source));
		    	context.write(newKey, mapKV);
		    }
	    // if finished or unchecked, emit through (identity)
		} else {
		Text identity = new Text(new String( distance + "|" + children + "|" + status + "|" + source));
		context.write(finalKey, identity);
	        }
	    } else {
		Text dummyVal = new Text(new String("#|" + children + "|#|#"));
		context.write(finalKey, dummyVal);
	    }
	}
    }

    public static class BFSReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
	public void reduce(LongWritable key, Iterable<Text> values, Context context)
	    throws IOException, InterruptedException {
	    String absoluteChildren = "";
	    HashSet finishedSet = new HashSet();
	    ArrayList copyList = new ArrayList();
	    isDone = true;

	    // Populate the finished hashset and find the dummy children value
	    for (Text value : values) {
	        String[] temp 		= value.toString().split("\\|");
	        String distance 	= temp[0];
	        String children 	= temp[1];
	        String status    	= temp[2];
	        String source  	 	= temp[3];
		// find the dummy children value
		if (distance.equals("#")) {
		    absoluteChildren = children;
		} // for all finished nodes, add to hash and emit
		else if (Integer.parseInt(status) == FINISHED) {
		    int sDistance = Integer.parseInt(distance);
		    int sSource   = Integer.parseInt(source);
		    finishedSet.add(sSource);
		    context.write(key, value);
		}
		copyList.add(value.toString());
	    }

	    // For unfinished nodes
	    for (int i=0; i < copyList.size(); i++) {
		String curr		= copyList.get(i).toString();
	        String[] temp 		= curr.split("\\|");
	        String distance 	= temp[0];
	        String children 	= temp[1];
	        String status    	= temp[2];
	        String source    	= temp[3];
		if (!distance.equals("#")) {
		    int sSource = Integer.parseInt(source);
		    // if it is gray, combine with absoluteChildren and emit
		    if (!finishedSet.contains(sSource) && Integer.parseInt(status)==(CHECKED)) {
			Text emitVal = new Text(new String(distance + "|" + absoluteChildren + "|" + status + "|" + source));
			isDone = false;
	 		context.write(key, emitVal);
		    // if it is white, just emit through (identity)
		    } else if (!finishedSet.contains(sSource) && Integer.parseInt(status)==(UNCHECKED)){    
			isDone = false;
			context.write(key, new Text(curr));
		    }
		} else {
		    Text dummyVal = new Text(new String("#|" + children + "|#|#"));
		    context.write(key, dummyVal);
		}
	    }
	}
    }

    // MAPPER FOR GRAPH
    public static class HistogramMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
	public void map(LongWritable key, Text value, Context context)
	    throws IOException, InterruptedException {
		String[] temp = value.toString().split("\\|");
		String[] distanceArray  = temp[0].split("\t");
		String distance = distanceArray[1];
		if (!distance.equals("-1") && !distance.equals("#")) {
		    long newLongKey = Long.parseLong(distance);
		    LongWritable newKey = new LongWritable(newLongKey);
		    context.write(newKey, new LongWritable(1));
	    	}
	}
    }

    // REDUCER FOR GRAPH
    public static class HistogramReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
	public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
	    throws IOException, InterruptedException {
		long sum = 0L;
		for (LongWritable value : values) {
		    sum += value.get();
		}
		context.write(key, new LongWritable(sum));
	}
    }


    // Shares denom argument across the cluster via DistributedCache
    public static void shareDenom(String denomStr, Configuration conf) {
        try {
	    Path localDenomPath = new Path(DENOM_PATH + "-source");
	    Path remoteDenomPath = new Path(DENOM_PATH);
	    BufferedWriter writer = new BufferedWriter(
				    new FileWriter(localDenomPath.toString()));
	    writer.write(denomStr);
	    writer.newLine();
	    writer.close();
	    FileSystem fs = FileSystem.get(conf);
	    fs.copyFromLocalFile(true,true,localDenomPath,remoteDenomPath);
	    DistributedCache.addCacheFile(remoteDenomPath.toUri(), conf);
        } catch (IOException ioe) {
            System.err.println("IOException writing to distributed cache");
            System.err.println(ioe.toString());
        }
    }


    public static void main(String[] rawArgs) throws Exception {
        GenericOptionsParser parser = new GenericOptionsParser(rawArgs);
        Configuration conf = parser.getConfiguration();
        String[] args = parser.getRemainingArgs();

        // Set denom from command line arguments
        shareDenom(args[2], conf);

        // Setting up mapreduce job to load in graph
        Job job = new Job(conf, "load graph");
        job.setJarByClass(SmallWorld.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(LoaderMap.class);
        job.setReducerClass(GrapherReduce.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Input from command-line argument, output to predictable place
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("bfs-0-out"));

        // Actually starts job, and waits for it to finish
        job.waitForCompletion(true);

        // Example of reading a counter
        System.out.println("Read in " + 
                   job.getCounters().findCounter(ValueUse.EDGE).getValue() + 
                           " edges");

        // Repeats your BFS mapreduce
        int i=0;
        // Will need to change terminating conditions to respond to data
        while (!isDone) {
	    if (i < MAX_ITERATIONS) {
            	job = new Job(conf, "bfs" + i);
            	job.setJarByClass(SmallWorld.class);

            	job.setMapOutputKeyClass(LongWritable.class);
            	job.setMapOutputValueClass(Text.class);
            	job.setOutputKeyClass(LongWritable.class);
            	job.setOutputValueClass(Text.class);

            	job.setMapperClass(BFSMapper.class);
            	job.setReducerClass(BFSReducer.class);

            	job.setInputFormatClass(TextInputFormat.class);
            	job.setOutputFormatClass(TextOutputFormat.class);

           	// Notice how each mapreduce job gets gets its own output dir
            	FileInputFormat.addInputPath(job, new Path("bfs-" + i + "-out"));
            	FileOutputFormat.setOutputPath(job, new Path("bfs-"+ (i+1) +"-out"));

            	job.waitForCompletion(true);
/*	    if (i != 0) {
	    	File lastFolder = new File("bfs-"+i+"-out");
	   	delete(lastFolder);
	    } */
            	i++;
	    }
        }

        // Mapreduce config for histogram computation
        job = new Job(conf, "hist");
        job.setJarByClass(SmallWorld.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(HistogramMapper.class);
     //   job.setCombinerClass(Reducer.class);            // <-- CombinerClass?
        job.setReducerClass(HistogramReducer.class); 

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // By declaring i above outside of loop conditions, can use it
        // here to get last bfs output to be input to histogram
        FileInputFormat.addInputPath(job, new Path("bfs-"+ i +"-out"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
	File finalFolder = new File("bfs-20-out");
	delete(finalFolder);
    }
}