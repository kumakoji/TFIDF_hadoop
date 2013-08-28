package hadoop_sample;

import hadoop_sample.TFIDF_hadoop;
import hadoop_sample.TFIDF_hadoop.Counters;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class TfIdfMapper
extends Mapper<LongWritable,Text,Text,Text> {
	@Override
	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
		String[] data = StringUtils.split(value.toString(),"\t");
		String KEY = data[0];
		String[] val = data[1].split(",");
		float sum = 0.0F;
		for(int i=0; i<val.length; i++){
			String[] pair = val[i].split(" ");
			sum += Float.parseFloat(pair[1]);
		}
		for(int i=0; i<val.length; i++){
			String[] pair = val[i].split(" ");
			context.write(new Text(pair[0]), new Text(KEY + " " + (Float.parseFloat(pair[1])/sum)));
		}	


	}
}





