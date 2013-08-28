package hadoop_sample;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.math.linear.OpenMapRealVector;
import org.apache.commons.math.linear.SparseRealVector;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * testesNormalizes each document vector by dividing each element by the
 * sum of all the elements. Passes to the Reducer.
 */
public class TranspositionMapper
    extends Mapper<LongWritable,Text,Text,Text> {
	private boolean NormalFlag;
	HashMap<String,String> lineSet = new HashMap<String,String>();
	//HashMap<String, String> keyvalue = new HashMap<String, String>();
	//ArrayList<String> key = new ArrayList<String>();
	//boolean flag=true;
	//float sum=0F;
	 @Override
	  public void setup(Context context)throws IOException, InterruptedException {
		 
		 NormalFlag = context.getConfiguration().getBoolean(TFIDF_hadoop.NORMAL_FLAG,NormalFlag);
		 System.err.println(NormalFlag);
	 }
	 
	 public void run(Context context)throws IOException, InterruptedException {
			setup(context);
			
			  while(context.nextKeyValue()){
					//keyvalue.put(context.getCurrentKey().toString(), context.getCurrentValue().toString());
					map(context.getCurrentKey(), context.getCurrentValue(),context);	
					//sum =0;
					//key.add(context.getCurrentKey().toString());
			  }
		}
	 
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  String[] lhsPair = StringUtils.split(value.toString(), "\t");
	  String[] valuePair = StringUtils.split(lhsPair[1].toString(), ",");

		     
		    for(int i=0; i<valuePair.length; i++){
		    	String[] valPair = StringUtils.split(valuePair[i]," ");
		    	System.err.println(valPair[0] + ":" + lhsPair[0] + " "+ valPair[1]);
		    	context.write(new Text(String.valueOf(valPair[0])), new Text(String.valueOf(lhsPair[0] + " " + valPair[1])));
		    }

  	}
}