package com.lunex.core.utils;

import java.util.ArrayList;
import java.util.List;

public class StatementVO {

	private String sql = "";
	private List<Object> params = new ArrayList<Object>();
	
	public String getSql() {
		return sql;
	}
	public void setSql(String sql) {
		this.sql = sql;
	}
	public List<Object> getParams() {
		return params;
	}
	public void setParams(List<Object> params) {
		this.params = params;
	}
	
}
