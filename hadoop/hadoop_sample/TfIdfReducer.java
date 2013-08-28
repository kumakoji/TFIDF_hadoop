package hadoop_sample;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.util.StringUtils;

/**
 * This is a simple identity reducer. It does no reduction, the record
 * is already created in the mapper, it simply picks the first (and only)
 * mapped record in the Iterable and writes it.
 */
public class TfIdfReducer extends Reducer<Text,Text,Text,Text> {
	
	Float DocNum;
	
	@Override
	public void setup(Context context)throws IOException, InterruptedException {

		DocNum= Float.parseFloat(context.getConfiguration().get(TFIDF_hadoop.DOC_NUM));
		System.err.println(DocNum);
	}
	
	@Override
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		float counter = 0.0F;
		ArrayList<String> data = new ArrayList<String>();
		for (Text value : values){
			data.add(value.toString());
			counter++;
		}
		String buf = "";
		System.err.println(key+"\t"+counter);
		for(int i=0; i<data.size(); i++){
			String[] pair = data.get(i).split(" ");
			if(buf.length() > 0) buf += ",";
			buf += pair[0] + " " + Math.log10(DocNum/counter)*Float.parseFloat(pair[1]);
		}
		context.write(key, new Text(buf.toString()));

	}
}