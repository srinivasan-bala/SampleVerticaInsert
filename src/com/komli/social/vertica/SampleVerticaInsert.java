package com.komli.social.vertica;
import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

public class SampleVerticaInsert {

	    public static void main(String[] args) throws IllegalArgumentException, IllegalAccessException
	    {
	    	  
	        try {
	            Class.forName("com.vertica.jdbc.Driver");
	        } catch (ClassNotFoundException e) {
	            System.err.println("Could not find the JDBC driver class.");
	            e.printStackTrace();
	            return;
	        }
	        
	        Properties myProp = new Properties();
	        myProp.put("user", "dbadmin");
	        myProp.put("password", "abc123");
	        //Use the properties below if you have to go through proxy
	        /*
	        myProp.put("proxy_host", "192.168.0.135");
            myProp.put("proxy_user", "sbalasubramanian");
            myProp.put("proxy_password", "komli@123");
            */
	        Connection conn;
	        
	        try {
	            conn = DriverManager.getConnection(
	                            "jdbc:vertica://192.168.0.135:5433/",
	                            myProp);	            
	            
	            // Disable auto commit
	            conn.setAutoCommit(false);
	            //conn.setSchema("public");
	            ArrayList<SocialVertica> svl=new ArrayList<SocialVertica>();
	            
	            for(int i=0;i<2;i++)
	            {
	            	SocialVertica sv=new SocialVertica();
		            svl.add(sv);
		            sv.timestamp=300;
		            //sv.fb_adgroup_id=300+i;
		            sv.advertiser_currency_id=500;
		            sv.advertiser_id=1;
		            sv.advertiser_io_currency_id=1;
		            sv.advertiser_io_id=1;
		            sv.advertiser_io_region_id=1;
		            sv.advertiser_li_id=300+i;
		            sv.advertiser_pricing_type=1;
		            sv.agency_id=1;
		            sv.campaign_category=1;
		            sv.creative_id=1;
		            sv.creative_offer_type_id=1;
		            sv.licensee_id=1;
		            sv.media_type=0;            
		            sv.fold_position=0;
		            sv.is_learning=0;
	            }
	            		
	            //Get Prepared Statement after setting all field values; In this statement all values
	            //are already bound
	            PreparedStatement stmt = SocialVertica.genBatchPrepareStmt(conn,svl);
	       
	            if(stmt!=null )
	            {
	            	stmt.executeBatch();
	            	int updateCnt=stmt.getUpdateCount();
	            	if(updateCnt>0)
	            	{
	            		System.out.println("Sucessfully inserted data! "+updateCnt);
	            	}else
	            	{
	            		System.out.println("Couldn't inserted data");
	            	}
	            }else
	            {
	            	System.out.println("Couldnt execute the statement");
	            }
	            conn.commit();
	            
	            conn.close();
	        } catch (SQLException e) {
	            e.printStackTrace();
	        }
	    }
}
