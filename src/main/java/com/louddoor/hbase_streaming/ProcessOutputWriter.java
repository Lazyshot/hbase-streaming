package com.louddoor.hbase_streaming;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;


public class ProcessOutputWriter extends Thread {
	private Queue<Object> msgQueue;
	private Process _proc;
	private OutputStream out;
	private BufferedWriter writeOut;
	private JsonFactory f = new JsonFactory();
	private JsonGenerator jg = null;
	
	private boolean procDied = false;
	private boolean finishUp = false;
	private boolean stop = false;
	
	ProcessOutputWriter(Process proc) throws IOException
	{
		msgQueue = new ConcurrentLinkedQueue<Object>();
		_proc = proc;
		
		out = _proc.getOutputStream();
		writeOut = new BufferedWriter(new OutputStreamWriter(out));
		jg = f.createJsonGenerator(writeOut);
	}
	
	
	public void write(Object msg) throws InterruptedException
	{
		if(msgQueue.size() > 200)
		{
			while(msgQueue.size() > 200)
			{
				Thread.sleep(5);
			}
		}
		
		msgQueue.add(msg);
		
	}
	
	public void stopThread()
	{
		stop = true;
	}
	
	public void setProcess(Process proc)
	{
		_proc = proc;
	}
	
	public void writeObject(Object msg) throws JsonGenerationException, IOException
	{
		if(msg instanceof String)
		{

			writeOut.write((String) msg);

			writeOut.flush();
		} 
		else if(msg instanceof Map) 
		{
			@SuppressWarnings("unchecked")
			Map<byte[], Map<byte[], byte[]>> mapMsg = (Map<byte[], Map<byte[], byte[]>>) msg;
			Iterator<Entry<byte[], Map<byte[], byte[]>>> i = mapMsg.entrySet().iterator();
			
			jg.writeStartObject();
			
			while(i.hasNext())
			{
				Entry<byte[], Map<byte[], byte[]>> ent = (Entry<byte[], Map<byte[], byte[]>>) i.next();
				
				jg.writeObjectFieldStart(Bytes.toString(ent.getKey()));
				
				Map<byte[], byte[]> inner = ent.getValue();
				Iterator<Entry<byte[], byte[]>> j = inner.entrySet().iterator();
				
				while(j.hasNext())
				{
					Entry<byte[], byte[]> innerEnt = (Entry<byte[], byte[]>) j.next();
					
					jg.writeStringField(Bytes.toString(innerEnt.getKey()), Bytes.toString(innerEnt.getValue()));
				}
				
				jg.writeEndObject();
			}
			
			jg.writeEndObject();
			
			jg.flush();
		}
	}
	
	public void run(){
		
		while(!stop)
		{
			if(msgQueue.size() > 0)
			{
				
				try {
					
					Object msg = msgQueue.peek();
					
					writeObject(msg);

					msgQueue.remove();
					
				} catch (IOException e) {
					
					if(e.getMessage().contains("pipe"))
					{
						procDied = true;
						stop = true;
					}
					
					e.printStackTrace();
					
				}
			}
		}
		
		if(finishUp)
		{
			while(msgQueue.size() > 0)
			{
				try {
					writeObject(msgQueue.poll());				
				} 
				catch(Exception e)
				{
					e.printStackTrace();
				}

			}
		}
		
		this.notifyAll();
	}
	
	public void finishUp(){
		
		finishUp = true;
		stop = true;
		
	}
	
	public boolean procDied()
	{
		return procDied;
	}
	
	
}