package com.nimbus.RecFlix.MahoutItemBased;

import java.io.File;
import java.io.IOException;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
//import org.apache.mahout.cf.taste.impl.model.jdbc.MySQLJDBCDataModel;


public class MovieDataModel extends FileDataModel{
        public final static String PERFERENCETABLE = "movie_preferences";
        public final static String USERID_COLUMN = "userID";
        public final static String ITEMID_COLUMN = "movieID";
        public final static String PERFERENCE_COLUMN = "preference";
       

        public MovieDataModel(String dataSourceName) throws TasteException, IOException {    
        	
                super(new File("filepath"));
        }

        public MovieDataModel() throws IOException {
        	super(new File("filepath"));
               // super(DBUtil.getDataSource(), PERFERENCETABLE, USERID_COLUMN, ITEMID_COLUMN, PERFERENCE_COLUMN);
        }
}

