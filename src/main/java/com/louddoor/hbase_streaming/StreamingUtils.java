package com.louddoor.hbase_streaming;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.*;
import org.json.JSONException;

public class StreamingUtils {
	
	public static void downloadFiles(@SuppressWarnings("rawtypes") Mapper.Context context) throws JSONException, IOException {
		downloadFiles(context.getConfiguration());
	}
	
	public static void downloadFiles(@SuppressWarnings("rawtypes") Reducer.Context context) throws JSONException, IOException {
		downloadFiles(context.getConfiguration());		
	}
	
	public static void downloadFiles(Configuration config) throws JSONException, IOException
	{
		saveFiles(config);
	}
	
	public static void saveFiles(Configuration conf) throws IOException
	{
		DistributedCache.getCacheFiles(conf);
	}
	
	
	public static Process buildProcess(String command) throws IOException {
		ProcessBuilder pb = new ProcessBuilder(command.split(" "));
		
		return pb.start();
	}
}