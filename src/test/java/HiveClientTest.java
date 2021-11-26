import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class HiveClientTest {

    @Test
    public void TestTableCreation() throws Exception {

        String basePath = "c:/ksingh/HiveClient/spark-warehouse/t1";
        SparkSession sparkSession = SparkUtils.getSparkSession();
        Dataset<Row> csv = sparkSession.read().option("header", "true").option("delimiter", ";").
                csv("src/test/resources/people.csv");

        HiveClient hiveClient = new HiveClient("src/test/resources/hive-site.xml");

        Table dummyTable = hiveClient.createDummyTable("default", "t5");
        hiveClient.getMetaStoreClient().createTable(dummyTable);
        if (hiveClient.getMetaStoreClient().tableExists(dummyTable.getDbName(), dummyTable.getTableName())) {
            hiveClient.getMetaStoreClient().alter_table(dummyTable.getDbName(), dummyTable.getTableName(),
                    dummyTable);

        } else {
            hiveClient.metaStoreClient.createTable(dummyTable);
        }

        List<String> keys = new ArrayList<>();
        keys.add("job");
        hiveClient.prepareTable("default", "t1",
                csv.schema(), keys, basePath);

        csv.write().partitionBy(keys.get(0)).mode(SaveMode.Append).
                parquet(basePath);

        /*sparkSession.sql(" ALTER TABLE default.t1 ADD PARTITION (job='Developer') " +
                "LOCATION 'c:/ksinghCode/java/HiveClient/spark-warehouse/t1/job=Developer' ");
        */

        List<Row> list = csv.select(csv.col(keys.get(0))).distinct().collectAsList();
        Table table = hiveClient.getMetaStoreClient().getTable("default", "t1");

        for (Row r : list) {
            String partitionKey = r.getString(0);
            List<String> value = new ArrayList<>();
            value.add(partitionKey);
            hiveClient.addPartition(table, value, String.format("/job=%s", partitionKey));
            sparkSession.sql("select * from default.t1 ").show(100, false);
        }
        sparkSession.sql("show partitions default.t1 ").show(100, false);

        System.out.println("test end");
    }
}