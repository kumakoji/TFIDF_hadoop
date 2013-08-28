package hadoop_sample;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.math.linear.OpenMapRealVector;
import org.apache.commons.math.linear.SparseRealVector;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * This is a simple identity reducer. It does no reduction, the record
 * is already created in the mapper, it simply picks the first (and only)
 * mapped record in the Iterable and writes it.
 */
public class TranspositionReducer extends Reducer<Text,Text,Text,Text> {
	
	
	//HashMap<String,String> lineSet = new HashMap<String,String>();
	//ArrayList<String> idinf = new ArrayList<String>();
	private boolean NormalFlag;
	@Override
	  public void setup(Context context)throws IOException, InterruptedException {
		 
		 NormalFlag = context.getConfiguration().getBoolean(TFIDF_hadoop.NORMAL_FLAG,NormalFlag);
	}
	
  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	  boolean omit_flag=false;
	  
	  StringBuilder buf = new StringBuilder();
	  for (Text value : values) {

		  System.err.println(key.toString()+":"+value.toString());
		  if(buf.length()!=0){
			  buf.append("," +value.toString());
			  omit_flag=true;  
		  }
		  else{
			  buf.append(value.toString());  
		  }
	  }
	  if(NormalFlag==false)omit_flag=true;
	  if(omit_flag)
	  context.write(new Text(key), new Text(buf.toString()));
	  
	  
}
}
