package org.apache.hudi.avro;

import java.io.EOFException;
import java.io.IOException;
import java.util.*;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.ResolvingDecoder;

public class CompatibleDatumReader<D> extends GenericDatumReader<D> {
	public CompatibleDatumReader(Schema writer, Schema reader) {
		super(writer, reader);
	}

	@Override
	protected Object readRecord(Object old, Schema expected, ResolvingDecoder in) throws IOException {

		Object r = getData().newRecord(old, expected);
		for (Field f : in.readFieldOrder()) {
			int pos = f.pos();
			String name = f.name();
			Object oldDatum = (old != null) ? getData().getField(r, name, pos) : null;
			try {
				getData().setField(r, name, pos, read(oldDatum, f.schema(), in));
			} catch (EOFException e) {
				// 如果没有更多的数据可读,说明read schema比write schema版本高,多出了新字段
				// 这种情况直接忽略新字段
				continue;
			}
		}
		return r;
	}

//		Map<String, String> removeField = new HashMap<>();
//		for (Field f : in.readFieldOrder()) {
//			int pos = f.pos();
//			String name = f.name();
//			Object oldDatum = (old != null) ? getData().getField(r, name, pos) : null;
//			try {
//				read(oldDatum, f.schema(), in);
//			} catch (EOFException e) {
//				removeField.put(name, name);
//				continue;
//			}
//		}

//		List<Schema.Field> fields = new ArrayList<>();
//		for (Schema.Field f : expected.getFields()) {
//			if (!removeField.containsKey(f.name())) {
//				Schema.Field _field = new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal());
//				fields.add(_field);
//			}
//		}
//
//		Schema record = Schema.createRecord(expected.getName(), expected.getDoc(), expected.getNamespace(), expected.isError(), fields);

////////////////////////////////////////////////////////////////////////////////////////
		//Object r1 = getData().newRecord(old, record);

//	  for (Field f : fields) {
//	   int pos = f.pos();
//	   String name = f.name();
//	   Object oldDatum = (old != null) ? getData().getField(r1, name, pos) : null;
//	   try {
//		getData().setField(r1, name, pos, read(oldDatum, f.schema(), in));
//	   } catch (EOFException e) {
//		continue;
//	   }
//	  }
//	  return r1;
//	 }
}