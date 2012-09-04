package com.louddoor.hbase_streaming;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.util.Date;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public class StreamingJob {
	
	static final String NAME = "";
	
	public static class StreamingMapper extends TableMapper<Text, Text>
	{		
		private Process proc = null;
		private Serializer serializer;
		
		BufferedReader readIn;
		BufferedReader errIn;
		
		InputStream in;
		InputStream err;
		
		private boolean stop = false;
		
		Text cKey = new Text();
		Text cVal = new Text();
		
		public void map(ImmutableBytesWritable rowKey, Result values, Context context) 
		throws IOException,  InterruptedException
		{
			try {
				stop = false;
				
				serializer.writeMap(rowKey, values);
				
				while(readIn.ready() && stop == false)
				{
					String readLine = readIn.readLine();
					
					if (readLine.equals("|next|"))
						stop = true;
					
					String[] lineParts = readLine.split("\t");
					String sval = "";

					for(int i = 0; i < lineParts.length; i++)
					{
						if(i == 0)
							cKey.set(lineParts[i]);
						else
							sval += lineParts[i] + "\t";
					}

					cVal.set(sval);

					context.write(cKey, cVal);
				}


				while(errIn.ready())
				{
					String errLine = errIn.readLine();
					
					if (errLine.equals(""))
						break;

					if(errLine.contains("reporter:counter"))
					{
						String[] parts = errLine.split(":")[2].split(",");

						context.getCounter(parts[0], parts[1]).increment(Long.parseLong(parts[2]));
					} else {
						System.err.println(errLine);
					}
				}
				
			} catch (Exception e) {
				e.printStackTrace();
				throw new InterruptedException(e.getMessage());
			}
		}

		public void setup(Context context)
		throws IOException
		{
			try {
				StreamingUtils.downloadFiles(context);
				setupProc(context);
			} catch (JSONException e) {
				e.printStackTrace();
			}
			
			String ser = context.getConfiguration().get("streaming.serializer", "json");
			
			if (ser.equals("json"))
			{
				serializer = new JsonSerializer(proc.getOutputStream());
			} else {
				serializer = new ByteSerializer(proc.getOutputStream());
			}
			
		}		
		public void setupProc(Context context) throws IOException{
			proc = StreamingUtils.buildProcess(context.getConfiguration().get("mapper.command"));
			
			in = proc.getInputStream();
			err = proc.getErrorStream();
			
			readIn = new BufferedReader(new InputStreamReader(in));
			errIn = new BufferedReader(new InputStreamReader(err));
		}
	}
	
	public static class StreamingReducer extends Reducer<Text, Text, Text, Text>
	{
		Process proc = null;
		OutputStream out;
		BufferedWriter writeOut;
		Serializer serializer;
		
		BufferedReader readIn;
		BufferedReader errIn;
		
		InputStream in;
		InputStream err;
		
		private boolean stop = false;
		
		Text cKey = new Text();
		Text cVal = new Text();
		
		public void reduce(Text id, Iterable<Text> values, Context context)
			throws IOException, InterruptedException 
		{
			try {
				serializer.writeReduce(id, values);
				
				while(readIn.ready() && stop == false)
				{
					String readLine = readIn.readLine();
					
					if (readLine.equals("|next|"))
						stop = true;
					
					String[] lineParts = readLine.split("\t");
					String sval = "";

					for(int i = 0; i < lineParts.length; i++)
					{
						if(i == 0)
							cKey.set(lineParts[i]);
						else
							sval += lineParts[i] + "\t";
					}

					cVal.set(sval);

					context.write(cKey, cVal);
				}


				while(errIn.ready())
				{
					String errLine = errIn.readLine();
					
					if (errLine.equals(""))
						break;

					if(errLine.contains("reporter:counter"))
					{
						String[] parts = errLine.split(":")[2].split(",");

						context.getCounter(parts[0], parts[1]).increment(Long.parseLong(parts[2]));
					} else {
						System.err.println(errLine);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
				throw new InterruptedException(e.getMessage());
			}
		}

		public void setup(Context context)
		throws IOException
		{
			try {
				StreamingUtils.downloadFiles(context);
				setupProc(context);
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
		
		public void setupProc(Context context) throws IOException{			
			proc = StreamingUtils.buildProcess(context.getConfiguration().get("mapper.command"));
			
			in = proc.getInputStream();
			err = proc.getErrorStream();
			
			readIn = new BufferedReader(new InputStreamReader(in));
			errIn = new BufferedReader(new InputStreamReader(err));
			
			String ser = context.getConfiguration().get("streaming.serializer", "json");
			
			if (ser.equals("json"))
			{
				serializer = new JsonSerializer(proc.getOutputStream());
			} else {
				serializer = new ByteSerializer(proc.getOutputStream());
			}
		}
	
	}
	
	@SuppressWarnings("static-access")
	public static Options setOptions() {
		Options options = new Options();
		
		options.addOption(new Option("help", "print this message"));
		
		Option configOption = OptionBuilder.withArgName("file").hasArg()
							.withDescription("set config file")
							.create("configFile");
		
		Option numReducers = OptionBuilder.withArgName("num").hasArg()
							.withDescription("Number of reducers")
							.create("numReducers");
		
		Option files = OptionBuilder.withArgName("file").hasArgs()
							.withDescription("All files required by job")
							.create("file");
		
		Option reducerCmd = OptionBuilder.withArgName("cmd").hasArgs()
							.withDescription("Reducer Command")
							.isRequired()
							.create("reducer");
		
		Option mapperCmd = OptionBuilder.withArgName("cmd").hasArgs()
							.withDescription("Mapper Command")
							.isRequired()
							.create("mapper");
		
		Option name = OptionBuilder.withArgName("name").hasArgs()
							.withDescription("Name of the job for reference")
							.create("name");
		
		Option serializer = OptionBuilder.withArgName("serializer").hasArgs()
							.withDescription("Serialization type (json/byte)")
							.create("serializer");
		
		options.addOption(configOption);
		options.addOption(files);
		options.addOption(numReducers);
		options.addOption(reducerCmd);
		options.addOption(mapperCmd);
		options.addOption(name);
		options.addOption(serializer);
		return options;
	}
	
	public static JSONObject loadConfig(String file) 
		throws JSONException, IOException 
	{
		return new JSONObject(readFullFile(file));	
	}
	
	public static String readFullFile(String file) 
		throws IOException 
	{
		FileReader in = new FileReader(file);
		StringBuilder contents = new StringBuilder();

		char[] buffer = new char[4096];

		int read = 0;

		do {
			contents.append(buffer, 0, read);
			read = in.read(buffer);
		} while (read >= 0);

		return contents.toString();
	}

	public static Job configureJob(Configuration conf, String [] args)
		throws Exception
	{
		Options options = setOptions();
		CommandLineParser parser = new GnuParser();
		CommandLine line = parser.parse(options, args);		
		Job job = new Job();
		Scan scan = new Scan();
		String table = "";
		String outFile = "";
		boolean overwrite = true;
		
		String name = line.getOptionValue("name");
		
		job.setJarByClass(StreamingJob.class);
		
		if(line.hasOption("help"))
		{
			HelpFormatter formatter = new HelpFormatter();
			
			formatter.printHelp("ant", options);
			
			System.exit(0);
		}
		
		String mapperCommand = line.getOptionValue("mapper");
		String reducerCommand = line.getOptionValue("reducer");
		String serializer = line.getOptionValue("serializer");
		
		job.getConfiguration().set("mapper.command", mapperCommand);
		job.getConfiguration().set("reducer.command", reducerCommand);
		
		if (serializer != null)
			job.getConfiguration().set("streaming.serializer", serializer);
		
		if (line.hasOption("file")) {
			
			FileSystem fs = FileSystem.get(conf);
			
			String dest = "/tmp/streaming_job/" + name + "_" +  new Date().getTime();
			String[] files = line.getOptionValues("file");
			
			fs.mkdirs(new Path(dest));
			
			for (String file : files)
			{
				Path s = new Path(file);
				Path d = new Path(dest + "/" + file);
				
				fs.copyFromLocalFile(s, d);
				
				DistributedCache.addCacheFile(new URI(dest + "/" + file + "#" + file), job.getConfiguration());
			}
			
			DistributedCache.createSymlink(job.getConfiguration());

		}
		
		
		String fileName = line.getOptionValue("config", "config.json");
		
		JSONObject config = loadConfig(fileName);
		
		if(config.has("input")){
			JSONObject input = config.getJSONObject("input");
			
			if(input.has("scan_caching"))
			{
				scan.setCaching(input.getInt("scan_caching"));
			}
			
			if(input.has("hbase_table"))
			{
				table = input.getString("hbase_table");
			}
			
			if(input.has("families"))
			{
				JSONArray families = input.getJSONArray("families");
				
				for(int i = 0; i < families.length(); i++)
				{
					scan.addFamily(Bytes.toBytes(families.getString(i)));
				}
			}
		}
		
		if(config.has("output"))
		{
			JSONObject output = config.getJSONObject("output");
			
			if(output.has("type"))
			{
				
			}
			
			if(output.has("path"))
			{
				outFile = output.getString("path");				
			}
			
			if(output.has("overwrite"))
			{
				overwrite = output.getBoolean("overwrite");
			}
		}
		
		if(config.has("name") && name == null)
		{
			name = config.getString("name");
		}
		
		if(name == null)
			name = NAME;
		
		job.setJobName(name);
		job.setInputFormatClass(TableInputFormat.class);
		
		TableMapReduceUtil.initTableMapperJob(table, scan, StreamingMapper.class, Text.class, Text.class, job);
		
		job.setReducerClass(StreamingReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		Configuration conffs = new Configuration();
		
		FileSystem fsA = FileSystem.get(conffs);
		
		Path outPath = new Path(outFile);

		if(fsA.exists(outPath)){
			if(overwrite)
			{
				fsA.delete(outPath, true);
			}
			else
			{
				throw new Exception("File already exists: " + outPath.toString());
			}
		}
		
		TextOutputFormat.setOutputPath(job, outPath);
		
		return job;
	}
	
	public static void main(String args[]) throws Exception 
	{
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		Job job = configureJob(conf, otherArgs);
				
		job.waitForCompletion(true);
		
	}
}