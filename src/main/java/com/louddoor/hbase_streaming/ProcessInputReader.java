package com.louddoor.hbase_streaming;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;



@SuppressWarnings("rawtypes")
public class ProcessInputReader<C extends TaskInputOutputContext> extends Thread {
	Process _proc;
	C _context;
	
	BufferedReader readIn;
	BufferedReader errIn;
	
	InputStream in;
	InputStream err;
	
	Text cKey = new Text();
	Text cVal = new Text();
	
	private boolean stop = false;
	
	ProcessInputReader(Process proc, C context)
	{
		
		_context = context;
		_proc = proc;
		
		in = proc.getInputStream();
		err = proc.getErrorStream();
		
		readIn = new BufferedReader(new InputStreamReader(in));
		errIn = new BufferedReader(new InputStreamReader(err));
		
	}
	
	public void stopThread()
	{
		stop = true;
	}

	@SuppressWarnings("unchecked")
	public void run()
	{
		try {
			while(stop == false)
			{
				while(readIn.ready())
				{
					String readLine = readIn.readLine();
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

					_context.write(cKey, cVal);
				}


				while(errIn.ready())
				{
					String errLine = errIn.readLine();

					if(errLine.contains("reporter:counter"))
					{
						String[] parts = errLine.split(":")[2].split(",");

						_context.getCounter(parts[0], parts[1]).increment(Long.parseLong(parts[2]));
					} else {
						System.err.println(errLine);
					}
				}
			}
			
			stop = false;
			_proc.destroy();

		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	
}