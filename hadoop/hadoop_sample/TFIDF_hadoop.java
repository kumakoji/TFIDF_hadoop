package hadoop_sample;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Hadoop Job to do Hierarchical Agglomerative (bottom up) clustering.
 * We start with a normalized set of term frequencies for each document.
 * At each stage, we find the two documents with the highest similarity
 * and merge them into a cluster "document". We stop when the similarity
 * between two documents is lower than a predefined threshold or when
 * the document set is clustered into a predefined threshold of clusters.
 */
public class TFIDF_hadoop {
	public static final int core = 30;
	public static boolean normal_flag = true;
	public static String NORMAL_FLAG="normal";


	//public static Long data_num = 0L;

	// keys: used by Mappers/Reducers
	public static final String DOC_NUM ="DocNum";

	// reporting:
	public enum Counters {REMAINING_RECORDS};
	private static void TfIdf(Configuration conf, Path firstQuery, Path query,Float DocNum)throws Exception {
		// TODO Auto-generated method stub
		Job job = new Job(conf, "tfidf/");
		job.getConfiguration().setFloat(DOC_NUM, DocNum);

		FileInputFormat.addInputPath(job, firstQuery);
		FileOutputFormat.setOutputPath(job, query);
		job.setJarByClass(TFIDF_hadoop.class);
		job.setMapperClass(TfIdfMapper.class);
		job.setReducerClass(TfIdfReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(1);
		boolean jobStatus = job.waitForCompletion(true);
		if (! jobStatus) {
			throw new Exception(job.getJobName() + " failed");
		}
	}

	private static void transposition(Configuration conf, Path dataInput,
			Path transOutput) throws Exception {
		Job job = new Job(conf, "transposition/");


		job.getConfiguration().setBoolean(NORMAL_FLAG, normal_flag);

		FileInputFormat.addInputPath(job, dataInput);
		FileOutputFormat.setOutputPath(job, transOutput);
		job.setJarByClass(TFIDF_hadoop.class);
		job.setMapperClass(TranspositionMapper.class);
		job.setReducerClass(TranspositionReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(core);
		boolean jobStatus = job.waitForCompletion(true);
		if (! jobStatus) {
			throw new Exception(job.getJobName() + " failed");
		}

	}

	/**
	 * This is how we are called.
	 * @param argv the input directory containing the raw TFs.
	 * @throws Exception if thrown.
	 */
	public static void main(String[] argv) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, argv).getRemainingArgs();
		System.out.println(otherArgs[0]);
		System.out.println("DocNum:"+otherArgs[1]);
		if (otherArgs.length < 1) {
			System.err.println("Usage hac <indir>");
			System.exit(-1);
		}
		Path indir = new Path(otherArgs[0]);
		Path basedir = indir.getParent();

		// phase 1: normalize the term frequency across each document
		//normalizeFrequencies(conf, indir, new Path(basedir, "temp0"));



		long start,stop;
		Float DocNum = Float.parseFloat(otherArgs[1]);
		String filename = otherArgs[2]+"/";
		int Matrix_number = 1;
		Path input = new Path(basedir, filename + "Matrix"+Matrix_number);
		Path output = new Path(basedir, filename + "Matrix_tfidf_trans");
		Path last_output = new Path(basedir, filename + "Matrix_tfidf");
		
		start = System.currentTimeMillis();
		System.out.println("makedata start");
		TfIdf(conf, input, output, DocNum);
		stop = System.currentTimeMillis();
		System.out.println("makedata Time: " + (stop-start));

		normal_flag=false;
		start = System.currentTimeMillis();
		System.out.println("transposition start");
		transposition(conf,output,last_output);
		stop = System.currentTimeMillis();
		System.out.println("transposition Time: " + (stop-start));
	}

}
