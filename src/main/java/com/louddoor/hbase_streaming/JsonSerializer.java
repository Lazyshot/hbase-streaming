package com.louddoor.hbase_streaming;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

public class JsonSerializer extends Serializer {

	private JsonFactory f = new JsonFactory();
	private JsonGenerator jg = null;

	public JsonSerializer(OutputStream out) {
		super(out);
		try {
			jg = f.createJsonGenerator(new OutputStreamWriter(out));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void writeMap(ImmutableBytesWritable rowKey, Result in) throws Exception {
		Iterator<Entry<byte[], NavigableMap<byte[], byte[]>>> i = in.getNoVersionMap().entrySet().iterator();

		jg.writeRaw(Bytes.toString(rowKey.get()) + "\t");

		jg.writeStartObject();

		while(i.hasNext())
		{
			Entry<byte[], NavigableMap<byte[], byte[]>> ent =  i.next();

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

		jg.writeRaw("\n");

		jg.flush();
	}

	@Override
	public void writeReduce(Text id, Iterable<Text> values) throws Exception {
		jg.writeRaw(id + "\t");

		Iterator<Text> i = values.iterator();

		jg.writeStartArray();

		while(i.hasNext())
		{
			Text val = i.next();

			jg.writeString(val.toString());
		}

		jg.writeEndArray();

		jg.writeRaw("\n");

		jg.flush();
	}
}