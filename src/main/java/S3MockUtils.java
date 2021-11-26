import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import io.findify.s3mock.S3Mock;
import lombok.SneakyThrows;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

public class S3MockUtils {


    public static StructType updateSchema(Dataset<Row> ds, List<String> binaryColumnName){
        StructField[] fields = ds.schema().fields();
        List<StructField> newSchemaFields = new ArrayList<>();

        for(StructField sf : fields){
            if(binaryColumnName.contains(sf.name())){
                newSchemaFields.add(new StructField(sf.name(), DataTypes.BinaryType,false,Metadata.empty()));
            } else{
                newSchemaFields.add(sf);
            }
        }
        return new StructType(newSchemaFields.toArray(new StructField[0]));
    }


    public static void main(String[] args) throws IOException {
        setup();
        SparkSession spark = SparkUtils.getSparkSession();
        // Replace Key with your AWS account key (You can find this on IAM
        spark.sparkContext()
                .hadoopConfiguration().set("fs.s3a.access.key", "test");
        spark.sparkContext()
                .hadoopConfiguration().set("fs.s3a.secret.key", "test");
        spark.sparkContext()
                .hadoopConfiguration().set("fs.s3a.endpoint", "http://localhost:8001");

        spark.sparkContext()
                .hadoopConfiguration().set("fs.s3a.path.style.access", "true");

        //RDD<String> stringRDD = spark.sparkContext().textFile("s3a://testbucket/src/main/resources/kvFile.txt", 1);
        Dataset<Row> csv = spark.read().option("header", "true").csv("s3a://testbucket/src/main/resources/kvFile.txt");

        List<String> binaryColumnName = Arrays.asList(new String[]{"filePath"});
        StructType newSchema = updateSchema(csv, binaryColumnName);
        String json = newSchema.json();


        JavaRDD<Row> rowJavaRDD = csv.javaRDD().mapPartitions(new FlatMapFunction<Iterator<Row>, Row>() {

            @Override
            public Iterator<Row> call(Iterator<Row> rowIterator) throws Exception {

                AwsClientBuilder.EndpointConfiguration endpoint =
                        new AwsClientBuilder.EndpointConfiguration("http://localhost:8001", "us-west-2");

                BasicAWSCredentials basicAWSCredentials = new BasicAWSCredentials("test", "test");

                AmazonS3 s3Client = AmazonS3ClientBuilder
                        .standard()
                        .withPathStyleAccessEnabled(true)
                        .withEndpointConfiguration(endpoint)
                        .withCredentials(new AWSStaticCredentialsProvider(basicAWSCredentials))
                        .build();

                DataType dataType = StructType.fromJson(json);
                Set<Integer> binaryColumnIndex = new HashSet<>();
                binaryColumnIndex.add(2);
                String zipFile = "src/main/resources/test.zip";
                String bucketName = "testbucket";
                byte[] buffer = new byte[1024];

                return new Iterator<Row>() {
                    @Override
                    public boolean hasNext() {
                        boolean hasNext = rowIterator.hasNext();
                        if (!hasNext) {
                            s3Client.shutdown();
                        }
                        return hasNext;
                    }

                    @SneakyThrows
                    @Override
                    public Row next() {
                        Row currentRow = rowIterator.next();
                        ArrayList<Object> newRow = new ArrayList<>();
                        StructType schema = (StructType) dataType;
                        StructField[] fields = schema.fields();

                        int i = 0;
                        for (StructField sf : fields) {
                            if (!sf.dataType().toString().equalsIgnoreCase("binarytype")) {
                                newRow.add(currentRow.get(i));
                            } else {
                                String filePath = currentRow.getString(i);
                                String fileName = Paths.get(filePath).getFileName().toString();
                                S3Object object = s3Client.getObject(new GetObjectRequest(bucketName, zipFile));
                                InputStream objectData = object.getObjectContent();
                                ZipInputStream zis = new ZipInputStream(objectData);

                                //this code block need to improvement
                                ZipEntry nextEntry = null;
                                while (true) {
                                    nextEntry = zis.getNextEntry();
                                    if (nextEntry == null)
                                        break;
                                    if (fileName.contains(nextEntry.getName())) {
                                        break;
                                    }
                                }

                                if (nextEntry != null) {
                                    int size = (int) nextEntry.getSize();

                                    byte[] mainBuffer = new byte[size];
                                    int bytesRead = 0;
                                    int start = 0;
                                    while ((bytesRead = zis.read(buffer)) > 0) {
                                        System.arraycopy(buffer, 0, mainBuffer, start, bytesRead);
                                        start += bytesRead;
                                    }
                                    newRow.add(mainBuffer);
                                } else {
                                    newRow.add("File Not Found".getBytes(StandardCharsets.UTF_8));
                                }
                            }
                            i++;
                        }
                        return RowFactory.create(newRow.toArray(new Object[0]));
                    }
                };
            }
        });



        String outPath = "src/main/resources/out/";
        List<Row> collect = rowJavaRDD.collect();

        Dataset<Row> dataFrame = spark.createDataFrame(rowJavaRDD, newSchema);



        for(Row r:collect){
            String name  = r.getString(0);
            byte[] bytes = r.<byte[]>getAs(2);
            OutputStream os = new FileOutputStream(new File(outPath + name));
            os.write(bytes);
            os.close();
        }

        // csv.show();
        shutdown();
    }

    public static S3Mock api = new S3Mock.Builder().withPort(8001).withInMemoryBackend().build();

    public static void setup() {
    /*
     S3Mock.create(8001, "/tmp/s3");
     */
        api.start();

        /* AWS S3 client setup.
         *  withPathStyleAccessEnabled(true) trick is required to overcome S3 default
         *  DNS-based bucket access scheme
         *  resulting in attempts to connect to addresses like "bucketname.localhost"
         *  which requires specific DNS setup.
         */
        AwsClientBuilder.EndpointConfiguration endpoint =
                new AwsClientBuilder.EndpointConfiguration("http://localhost:8001", "us-west-2");

        BasicAWSCredentials basicAWSCredentials = new BasicAWSCredentials("test", "test");

        AmazonS3 client = AmazonS3ClientBuilder
                .standard()
                .withPathStyleAccessEnabled(true)
                .withEndpointConfiguration(endpoint)
                .withCredentials(new AWSStaticCredentialsProvider(basicAWSCredentials))
                .build();

        client.createBucket("testbucket");
        client.putObject("testbucket", "file/name", "contents");

        client.putObject("testbucket", "src/main/resources/test.zip", new File("src/main/resources/test.zip"));
        client.putObject("testbucket", "src/main/resources/kvFile.txt", new File("src/main/resources/kvFile.txt"));

        System.out.println("test");
    }

    public static void shutdown() {
        api.shutdown(); // kills the underlying actor system. Use api.stop() to just unbind the port.
    }

    public static String basePath(String bucketName) {
        return String.format("s3a://%s", bucketName);
    }

}
