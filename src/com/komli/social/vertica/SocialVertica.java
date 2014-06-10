package com.komli.social.vertica;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.lang.reflect.Field;

/*
 * A simple class to encapsulate all the fields which will be inserted as part of social, and generate the SQL 
 * required to insert into  vertica hourly table.
 * Why are we using this?
 * We needed a simple class without the layered overhead  and dependency headache associated with   
 * stuff like hibernate. The velocity of changes required in this table is much less compared to API. So whenever
 * a new field associated with social has to be introduced we just add a new variable and generate JDBC style
 * insert statement to insert into vertica; At this point this class assumes only single inserts and can be easily 
 * converted to batch inserts. 
 */
public class SocialVertica {
	/*
	 * All public variables in this class are assumed to be the fields which needs to be inserted into vertica. Some
	 * rules for naming these variables
	 * 1)The variable name should be same as vertica column.
	 * 2)All variable should be declared in a separate line. 
	 * Why are we writing each field in a separate line? To make it easy to search and help the maintenance of code.
	 * Also all variables are public, which allows the calling layer to easily set instead of classical get/set methods
	 */
	public int timestamp;
	public int licensee_id;
	public int agency_id;
	public int advertiser_id;
	public int advertiser_currency_id;
	public int advertiser_io_region_id;
	public int advertiser_io_id;
	public int advertiser_io_currency_id;
	public int advertiser_pricing_type;
	public int advertiser_li_id;
	public int creative_id;
	public int creative_offer_type_id;
	public int campaign_category;
	public int media_type;
	public int fold_position;
	public int is_learning;
	
	//Any variable which needs to be inserted into vertica has to be added with exactly the "same variable name"
	//as shown below; The prepare statment is formed on the basis of these variables.
	
	private static ArrayList<String> varBindOrder=new ArrayList<String>(Arrays.asList("timestamp","licensee_id",
			"agency_id","advertiser_id","advertiser_currency_id","advertiser_io_region_id","advertiser_io_id",
			"advertiser_io_currency_id","advertiser_pricing_type","advertiser_li_id","creative_id","creative_offer_type_id",
			"campaign_category","media_type","fold_position","is_learning"));
	
	//private static ArrayList<String> varBindOrder=new ArrayList<String>(Arrays.asList("timestamp","fold_position","is_learning"));
	
	private StringBuffer pSb;
	SocialVertica()
	{
		//Create insert string
		pSb=new StringBuffer();
		pSb.append("INSERT INTO public.HOURLY ( ");
		for(String s:varBindOrder)
		{
			pSb.append(s+",");
		}
		//Remove the last ","
		pSb.deleteCharAt(pSb.length()-1);
		pSb.append(") VALUES (");
		for(String s:varBindOrder)
		{
			pSb.append("? "+",");
		}
		pSb.deleteCharAt(pSb.length()-1);
		pSb.append(")");
		System.out.println(pSb.toString());
	}
	/**
	 * This routine creates a prepared statement and binds variable values; The prepared statement
	 * can be executed immediately.
	 * @param con: Connection 
	 * @return PreparedStatement: A prepared bound sql statement which can be executed.
	 */
	public PreparedStatement  generatePreparedStatement(Connection con)
	{
		try
		{
			PreparedStatement pStmt = con.prepareStatement(pSb.toString());	
			int varOrder=0;
			for(String var:varBindOrder)
			{
				Field f=this.getClass().getField(var);
				varOrder++;
				Class<?> type = f.getType();
				if(type==int.class)
				{
					pStmt.setInt(varOrder, f.getInt(this));
				}else if(type==Double.class)
				{
					pStmt.setDouble(varOrder,f.getDouble(this));
				}else if(type==Float.class)
				{
					pStmt.setFloat(varOrder,f.getFloat(this));
				}else
				{
					System.out.println("Unknown type!");
					return null;
				}
			}
			return pStmt;
		}catch(SQLException|NoSuchFieldException|IllegalAccessException ex)
		{
			System.out.println(ex.getMessage());
		}
		return null;
	}
}
