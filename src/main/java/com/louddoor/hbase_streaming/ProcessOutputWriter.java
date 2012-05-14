package com.louddoor.hbase_streaming;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;


public class ProcessOutputWriter extends Thread {
	private Queue<String> msgQueue;
	private Process _proc;
	private OutputStream out;
	private BufferedWriter writeOut;
	
	private boolean procDied = false;
	private boolean finishUp = false;
	private boolean stop = false;
	
	ProcessOutputWriter(Process proc)
	{
		msgQueue = new ConcurrentLinkedQueue<String>();
		_proc = proc;
		
		out = _proc.getOutputStream();
		writeOut = new BufferedWriter(new OutputStreamWriter(out));
	}
	
	
	public void writeMsg(String msg) throws InterruptedException
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
	
	public void run(){
		
		while(!stop)
		{
			if(msgQueue.size() > 0)
			{
				try {
					
					String msg = msgQueue.peek();
					
					writeOut.write(msg);
					
					writeOut.flush();
					
					msgQueue.poll();
					
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
					String msg = msgQueue.poll();

					writeOut.write(msg);

					writeOut.flush();
				
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