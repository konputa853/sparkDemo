package com.spark.demo;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;


public class Main {

    public static long nullCountInColumn(Dataset<Row> df, String columnName)
    {
        Dataset<Row> null_filtered_df = df.filter(df.col(columnName).isNull());
        return null_filtered_df.count();
    }


    public static void main(String[] args) 
    {
        //Create spark session
        SparkSession spark = SparkSession.builder().appName("EmployeeProcessDemo").master("local").getOrCreate();
        String inputFilePath = "/home/iceman/Documents/projects/sparkDemo/demo/src/main/resources/raw/employee/20240801/employee.csv";
        
        //Create input dataframe
        Dataset<Row> df = spark.read().option("header",true).csv(inputFilePath);
        df.printSchema();

        //Check if employee id has null values. Hard fail the pipeline if there are null values
        long employee_id_null_count = nullCountInColumn(df, "EMPLOYEE_ID");
        if(employee_id_null_count>0)
        {
            System.out.println("Failing the pipeline. Null values found. Total count:"+employee_id_null_count);
            System.exit(1);
        }
        else
        {
            System.out.println("No Null values found");
        }

        //Check if last name column has null values. Throw a warning if there are null values
        long last_name_null_count = nullCountInColumn(df, "LAST_NAME");
        if(last_name_null_count>0)
        {
            System.out.println("Warning: Null values found. Total count:"+last_name_null_count);
        }
        else
        {
            System.out.println("No Null values found");
        }

        //Filter out the rows where the department id is negative
        long missing_department_id = nullCountInColumn(df, "DEPARTMENT_ID");
        if(missing_department_id>0)
        {
            System.out.println("Warning: Null values found. Total count:"+last_name_null_count);
            System.out.println("Warning: removing bad rows");
            System.out.println("Cunt before dropping bad rows:"+df.count());
            df = df.filter(df.col("DEPARTMENT_ID").isNotNull());
            System.out.println("Cunt after dropping bad rows:"+df.count());
        }
        else
        {
            System.out.println("No Null values found");
        }

        String outputFilePath = "/home/iceman/Documents/projects/sparkDemo/demo/src/main/resources/cleansed/employee/20240801/employee.csv";
        df.write().csv(outputFilePath);

        

    }
}