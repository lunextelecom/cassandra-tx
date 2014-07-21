package com.lunex.core.utils;

import java.io.StringReader;
import java.security.MessageDigest;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.UUID;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;

public class SQLParser {

	static CCJSqlParserManager parserManager = new CCJSqlParserManager();

	public static void main(String[] args) throws JSQLParserException
    {
         
    	 new SQLParser();

    }
    public SQLParser() throws JSQLParserException
    {
        String statement = "insert into test_keyspace.customer(username, firstname, lastname, age) values('trinhtran',?,?,24)";
        Statement stm = parserManager.parse(new StringReader(statement));
        if (stm instanceof Select) {
			System.out.println("select");
			
		}else if(stm instanceof Update){
			Update detailStm =  (Update) stm; 
			
			Table t = detailStm.getTable();
			String txName = "txTable";
			String tmpSql = detailStm.toString().toLowerCase();
			tmpSql = tmpSql.replaceFirst("update " + t.getWholeTableName(), "select * from " + txName);
			String whereSql = detailStm.getWhere().toString();
			statement = statement.replaceFirst(t.getWholeTableName(), txName) + " and cstx_id_ = ?";
			System.out.println(tmpSql);
			System.out.println(statement);
			
		}else if(stm instanceof Delete){
			System.out.println("delete");
			
		}else if(stm instanceof Insert){
			System.out.println("insert");
			Insert info = ((Insert) stm);
			info.getColumns().add("ctsx_is");
			System.out.println(info.toString());
			String tableName = ((Insert) stm).getTable().getName();
			
			
		}
        
    }
    public static void testInsert(){
    	try {
    		StringBuilder insertSql = new StringBuilder();
			String statement = "insert into a(col1, col2, col3, col4, clo5) values(?,?,now(),1,'a')";
			CCJSqlParserManager parserManager = new CCJSqlParserManager();
			Statement stm = parserManager.parse(new StringReader(statement));
			if (stm instanceof Insert) {
				Insert detailStm =  (Insert) stm; 
				insertSql.append("insert into " + detailStm.getTable()+"_tx" + "(");
				Boolean isFirst = true;
				for (Object obj : detailStm.getColumns()) {
					if(isFirst){
						isFirst = false;
						insertSql.append(((Column)obj).getColumnName());
					}else{
						insertSql.append("," + ((Column)obj).getColumnName());
					}
				}
				insertSql.append(") values(");
				isFirst = true;
				ItemsList list =  detailStm.getItemsList();
				/*for (Object obj : detailStm.getItemsList()) {
					if(isFirst){
						isFirst = false;
						insertSql.append(((Column)obj).getColumnName());
					}else{
						insertSql.append("," + ((Column)obj).getColumnName());
					}
				}*/
				ExpressionList list1 = (ExpressionList) list;
				ArrayList<Object> arr = (ArrayList<Object>) list1.getExpressions();
				
				insertSql.append(")");
				System.out.println("insert");
				
			}
		} catch (JSQLParserException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    
}
