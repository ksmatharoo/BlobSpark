import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import javax.xml.bind.SchemaOutputResolver;
import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

public class ZipReader {

    public static void main(String[] args) throws IOException {

        test();
        System.exit(0);

        String filePath = "src/main/resources/test.zip";
        String outputPath = "src/main/resources/out";

        ZipInputStream zis = new ZipInputStream(new FileInputStream(filePath));
        ZipEntry nextEntry = zis.getNextEntry();

        byte[] buffer = new byte[1024];
        int offset = 0;
        while (nextEntry != null) {
            String name = nextEntry.getName();
            int size = (int) nextEntry.getSize();
            FileOutputStream fileOutputStream = new FileOutputStream(outputPath + File.separatorChar
                    + name);
            int read = 0;

            while ((read = zis.read(buffer)) > 0) {
                fileOutputStream.write(buffer, 0, read);
            }
            fileOutputStream.close();
            nextEntry = zis.getNextEntry();

        }
    }

    public static void test() throws IOException {

        String filePath = "src/main/resources/test.zip";
        String outputPath = "src/main/resources/out";
        ZipFile zf = new ZipFile(filePath);
        String[] list = {"hive-site.xml", "people.csv"};
        byte[] buffer = new byte[1024];
        try {
            for (String name : list) {
                InputStream in = zf.getInputStream(zf.getEntry(name));
                FileOutputStream fos = new FileOutputStream(outputPath + File.separatorChar
                        + name);
                try {
                    int size = 0;
                    while ((size = in.read(buffer)) > 0) {
                        fos.write(buffer, 0, size);
                    }

                } finally {
                    in.close();
                    fos.close();
                }
            }
        } finally {
            zf.close();
        }


        /*Path zipfile = Paths.get(filePath);
        FileSystems.newFileSystem(zipfile, env, null);*/
    }

    public static void getInputStreamFromS3(String bucketName,String key) throws IOException {
        AmazonS3 s3Client = new AmazonS3Client(new ProfileCredentialsProvider());
        S3Object object = s3Client.getObject(new GetObjectRequest(bucketName, key));
        InputStream objectData = object.getObjectContent();

        ZipInputStream zis = new ZipInputStream(objectData);
        ZipEntry nextEntry = zis.getNextEntry();

        String outputPath = "test";

        byte[] buffer = new byte[1024];
        int offset = 0;
        while (nextEntry != null) {
            String name = nextEntry.getName();
            int size = (int) nextEntry.getSize();
            FileOutputStream fileOutputStream = new FileOutputStream(outputPath + File.separatorChar
                    + name);
            int read = 0;

            while ((read = zis.read(buffer)) > 0) {
                fileOutputStream.write(buffer, 0, read);
            }
            fileOutputStream.close();
            nextEntry = zis.getNextEntry();

        }

        // Process the objectData stream.
        objectData.close();

    }

}
