
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import java.io.*;
import java.util.*;

public class BlobReader {

    public static void main(String[] args) throws Exception {

        boolean writeToFile = true;
        boolean isHiveTableTest = !(writeToFile);

        S3MockUtils.setup();

        SparkSession spark = SparkUtils.getSparkSession();
        setS3Config(spark);

        Dataset<Row> csv = spark.read().option("header", "true")
                .csv("s3a://testbucket/src/main/resources/kvFile.txt");

        List<String> binaryColumnName = Arrays.asList(new String[]{"filePath"});
        StructType newSchema = updateSchema(csv, binaryColumnName);

        String endPointUrl = "http://localhost:8001";
        String json = newSchema.json();
        String zipFile = "src/main/resources/test.zip";
        String bucketName = "testbucket";

        JavaRDD<Row> rowJavaRDD = csv.javaRDD().mapPartitions(new BlobReaderMapFunction(endPointUrl,json,zipFile,bucketName));

        Dataset<Row> dataFrame = spark.createDataFrame(rowJavaRDD, newSchema);

        //hive table test
        if(isHiveTableTest) {
            writeToHive(spark, dataFrame);
        }

        //output test
        if(writeToFile) {

            String outPath = "src/main/resources/out/";
            List<Row> collect = rowJavaRDD.collect();
            for (Row r : collect) {
                String name = r.getString(0);
                byte[] bytes = r.<byte[]>getAs(2);
                OutputStream os = new FileOutputStream(new File(outPath + name));
                os.write(bytes);
                os.close();
            }
        }

        // csv.show();
        S3MockUtils.shutdown();
    }

    public static void writeToHive(SparkSession spark,Dataset<Row> dataset) throws Exception {
        String basePath = "c:/ksingh/HiveClient/spark-warehouse/t2";
        HiveClient hiveClient = new HiveClient("src/test/resources/hive-site.xml");

        hiveClient.prepareTable("default", "t2",
                dataset.schema(), Arrays.asList(new String[]{"number"}), basePath);
        dataset.write().partitionBy("number").mode(SaveMode.Append).
                parquet(basePath);
        Table table = hiveClient.getMetaStoreClient().getTable("default", "t2");
        hiveClient.addPartition(table, Arrays.asList(new String[]{"1"}), "/number=1");

        spark.sql("select * from default.t2 ").show(100, false);

    }

    public static StructType updateSchema(Dataset<Row> ds, List<String> binaryColumnName) {
        StructField[] fields = ds.schema().fields();
        List<StructField> newSchemaFields = new ArrayList<>();

        for (StructField sf : fields) {
            if (binaryColumnName.contains(sf.name())) {
                newSchemaFields.add(new StructField(sf.name(), DataTypes.BinaryType, false, Metadata.empty()));
            } else {
                newSchemaFields.add(sf);
            }
        }
        return new StructType(newSchemaFields.toArray(new StructField[0]));
    }

    public static void setS3Config(SparkSession spark){
        spark.sparkContext()
                .hadoopConfiguration().set("fs.s3a.access.key", "test");
        spark.sparkContext()
                .hadoopConfiguration().set("fs.s3a.secret.key", "test");
        spark.sparkContext()
                .hadoopConfiguration().set("fs.s3a.endpoint", "http://localhost:8001");
        spark.sparkContext()
                .hadoopConfiguration().set("fs.s3a.path.style.access", "true");
    }

}
