package com.louddoor.hbase_streaming;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.json.JSONException;
import org.json.JSONObject;

public class StreamingUtils {
	
	public static void downloadFiles(@SuppressWarnings("rawtypes") Mapper.Context context) throws JSONException, IOException {
		downloadFiles(context.getConfiguration());
	}
	
	public static void downloadFiles(@SuppressWarnings("rawtypes") Reducer.Context context) throws JSONException, IOException {
		downloadFiles(context.getConfiguration());		
	}
	
	public static void downloadFiles(Configuration config) throws JSONException, IOException
	{
		saveFiles(new JSONObject(config.get("files")));
	}
	
	public static void saveFiles(JSONObject files) throws JSONException, IOException
	{
		@SuppressWarnings("unchecked")
		Iterator<String> it = files.keys();
		
		while(it.hasNext())
		{
			String file = it.next();
			String contents = files.getString(file);
			
			FileWriter out = new FileWriter(file, false);
			BufferedWriter writ = new BufferedWriter(out);
			
			writ.write(contents);			
		}
	}
	
	
	public static Process buildProcess(String command) throws IOException {
		ProcessBuilder pb = new ProcessBuilder(command.split(" "));
		
		return pb.start();
	}
}