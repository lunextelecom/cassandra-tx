package com.lunex.core.utils;

import java.util.List;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;

public class RowKey {

	private List<ColumnMetadata> keys;
	
	private Row row;

	public Row getRow() {
		return row;
	}

	public void setRow(Row row) {
		this.row = row;
	}

	public List<ColumnMetadata> getKeys() {
		return keys;
	}

	public void setKeys(List<ColumnMetadata> keys) {
		
		this.keys = keys;
	}
	
	public boolean equals(Object obj) {
       if (!(obj instanceof RowKey))
            return false;
        if (obj == this)
            return true;

        RowKey cmp = (RowKey) obj;
        
        for (ColumnMetadata colKey : keys) {
        	String colType = colKey.getType().getName().toString();
			String colName = colKey.getName();
			if(!getValue(row, colType, colName).equals(getValue(cmp.getRow(), colType, colName))){
				return false;
			}
		}
        return true;
    }
	
	public int hashCode() {
		return 1;
    }
	
	private Object getValue(final Row row, String colType,
			String colName) {
		if(colType.equals(DataType.ascii().getName().toString())
				|| colType.equals(DataType.text().getName().toString())
				|| colType.equals(DataType.varchar().getName().toString())
				){
			return row.getString(colName);
		}else if(colType.equals(DataType.uuid().getName().toString()) || colType.equals(DataType.timeuuid().getName().toString())){
			return row.getUUID(colName);
		}else if(colType.equals(DataType.timestamp().getName().toString())){
			return row.getDate(colName);
		}else if(colType.equals(DataType.cboolean().getName().toString())){
			return row.getBool(colName);
		}else if(colType.equals(DataType.blob().getName().toString())){
			return row.getBytes(colName);
		}else if(colType.equals(DataType.cdouble().getName().toString())){
			return row.getDouble(colName);
		}else if(colType.equals(DataType.cfloat().getName().toString())){
			return row.getFloat(colName);
		}else if(colType.equals(DataType.cint().getName().toString())){
			return row.getInt(colName);
		}else if(colType.equals(DataType.decimal().getName().toString())){
			return row.getDecimal(colName);
		}else if(colType.equals(DataType.inet().getName().toString())){
			return row.getInet(colName);
		}else if(colType.equals(DataType.counter().getName().toString()) || colType.equals(DataType.bigint().getName().toString())){
			return row.getLong(colName);
		}
		return null;
	}
}
