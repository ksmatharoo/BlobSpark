import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import lombok.SneakyThrows;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class BlobReaderMapFunction implements FlatMapFunction<Iterator<Row>, Row> {

    String endPointUrl ;
    String schemaJson;
    String zipFile ;
    String bucketName ;

    public BlobReaderMapFunction(String endPointUrl, String schemaJson, String zipFile, String bucketName) {
        this.endPointUrl = endPointUrl;
        this.schemaJson = schemaJson;
        this.zipFile = zipFile;
        this.bucketName = bucketName;
    }

    @Override
    public Iterator<Row> call(Iterator<Row> rowIterator) throws Exception {

        AwsClientBuilder.EndpointConfiguration endpoint =
                new AwsClientBuilder.EndpointConfiguration(endPointUrl, "us-west-2");

        BasicAWSCredentials basicAWSCredentials = new BasicAWSCredentials("test", "test");

        AmazonS3 s3Client = AmazonS3ClientBuilder
                .standard()
                .withPathStyleAccessEnabled(true)
                .withEndpointConfiguration(endpoint)
                .withCredentials(new AWSStaticCredentialsProvider(basicAWSCredentials))
                .build();

        DataType dataType = StructType.fromJson(schemaJson);
        Set<Integer> binaryColumnIndex = new HashSet<>();
        binaryColumnIndex.add(2);

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
}

