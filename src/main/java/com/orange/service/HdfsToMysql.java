package com.orange.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.orange.common.util.Constants;
import com.orange.common.util.DateUtils;
import com.orange.common.util.JdbcUtil;

public class HdfsToMysql {

	
	
	public static void main(String[] args) {
		
		 SparkSession spark = SparkSession
			      .builder()
			      .appName("Java Spark SQL Data Sources Example")
			      .config("spark.some.config.option", "some-value")
			      .getOrCreate();
		 
		 JavaRDD<String> hdfsRDD = spark.read().textFile("hdfs://master:9000/SDKData/total/data" + DateUtils.getYesterdayDate() + "/streamingdata" + DateUtils.getYesterdayDate() + ".txt").toJavaRDD();
		 
		  String schemaString = "user_id class_id carme_id start_time end_time use_time";
			
		    List<StructField> fields = new ArrayList<StructField>();
		    for (String fieldName : schemaString.split(" ")) {
		      StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
		      fields.add(field);
		    }
		    
		    StructType schema = DataTypes.createStructType(fields);
		    
		    JavaRDD<Row> rowRDD = hdfsRDD.map(new Function<String, Row>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Row call(String row) throws Exception {
					String[] attr = row.split(",");
					if(attr.length ==6){
						return RowFactory.create(attr[0],attr[1],attr[2],attr[3],attr[4],attr[5]);
					}else{
						return RowFactory.create(null,null,null,null,null,null);
					}  
			} 
			});
		    if(!rowRDD.equals(null) && !rowRDD.equals("")){
		    Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);
			
		    peopleDataFrame.createOrReplaceTempView("streamingData");
		    Dataset<Row> resultDF = spark.sql("select  user_id,class_id,carme_id,start_time,end_time,use_time from streamingData");
		    
		    resultDF.write().mode("append").jdbc(Constants.URL_TEST, Constants.JDBC_LIVING_STREAMING, JdbcUtil.JdbcCon());
		    }
		 spark.stop();
	}
}
