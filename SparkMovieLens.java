/**
 * 
 */
package default1;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.COL;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.Dataset;
import static org.apache.spark.sql.functions.col;

import org.apache.commons.math3.stat.descriptive.summary.Sum;

/**
 * @author lenovo
 *
 */
public class SparkMovieLens {

	/**
	 * Spark Session to be used as a instrument as a starting point of application
	 */
	public SparkMovieLens() {

	}

	/**
	 * @param args
	 * @throws AnalysisException 
	 */
	public static void main(String[] args) throws AnalysisException {
		
		org.apache.spark.sql.SparkSession spark=org.apache.spark.sql.SparkSession.builder().appName("hello").config("spark.master", "local").getOrCreate();
	
	
		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(spark);
			
		Dataset<Row> moviedata = spark.read().option("header","true").format("com.databricks.spark.csv")
				.csv("C:\\Users\\lenovo\\Desktop\\movie_use_case.csv");
		
		
		Dataset<Row> userdata = spark.read().option("header","true").format("com.databricks.spark.csv")
				.csv("C:\\Users\\lenovo\\Desktop\\user_table.csv");
		
		sqlContext.registerDataFrameAsTable(moviedata,"movie_table");
		
		sqlContext.registerDataFrameAsTable(userdata,"user_table");
		

		
		
		Dataset<Row> movie_use_case=sqlContext.sql("select user_id,movie_name,rating_id,movie_id from movie_table");
		
		Dataset<Row> user_table=sqlContext.sql("select * from user_table");
		
		
		Dataset<Row> user_table1=movie_use_case.join(user_table,"user_id");
		
		
		user_table1.show();
		
		
		//part 1 list all the users 
		user_table1.select("movie_name","rating_id").show();
		
		//part 2 movie and number of rating
		user_table1.select("user_name","rating_id").show();
		
		
		// part 3 list all the movie id 
		user_table1.select("movie_id").show();


	}
	
	
}
