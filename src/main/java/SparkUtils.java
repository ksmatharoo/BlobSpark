import org.apache.spark.sql.SparkSession;

public class SparkUtils {

    public static SparkSession getSparkSession() {
        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("Java Spark Hive Example")
                .enableHiveSupport()
                .getOrCreate();

        return spark;
    }
}
