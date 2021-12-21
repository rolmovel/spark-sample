package com.example.app;

import com.example.app.chain.Tasks;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testGetLoanByAge()
    {
        SparkSession session = SparkSession
            .builder()
            .appName("bank")
            .master("local[1]")
            .getOrCreate();

        // LOAD DATASETS
        Dataset<Row> dataframe = session
            .read()
            .option("header", "true")
            .option("inferSchema", true)
            .csv("src/test/resource/Test1.csv");

        Dataset output = Tasks.getLoanByAge(dataframe);

        assertTrue( true );
    }
}
