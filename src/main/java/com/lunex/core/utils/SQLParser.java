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

    public SQLParser() throws JSQLParserException
    {
        String statement = "Update db.table1 set a = now(), b = 1 where id = 10 and ab ='dfds'";
        Statement stm = parserManager.parse(new StringReader(statement));
        if (stm instanceof Select) {
			System.out.println("select");
			
		}else if(stm instanceof Update){
			Update detailStm =  (Update) stm; 
			Table t = detailStm.getTable();
			String whereSql = detailStm.getWhere().toString();
			
			System.out.println(t.getWholeTableName());
			System.out.println(whereSql);
			
		}else if(stm instanceof Delete){
			System.out.println("delete");
			
		}else if(stm instanceof Insert){
			System.out.println("insert");
			
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
    public static void main(String[] args) throws JSQLParserException
    {
         
         String sql = " delete      from customer whree fd=dfdsf".toLowerCase();
 		sql = sql.replaceFirst("delete ", "select * ");
 		System.out.println(sql);

    }
    
}
