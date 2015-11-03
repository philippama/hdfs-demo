package pm.hdfs.playground;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import pm.hdfs.TestConfig;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class HdfsTest {

    private FileSystem fileSystem;

    @Before
    public void hadoopStuff() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", TestConfig.HDFS_URL);
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        fileSystem = FileSystem.get(conf);

        Path testDirectoryPath = new Path(TestConfig.HDFS_DIRECTORY);
        if (!fileSystem.isDirectory(testDirectoryPath)) {
            fileSystem.mkdirs(testDirectoryPath);
        }
    }

    @After
    public void closeHadoopStuff() throws IOException {
        fileSystem.close();
    }

    @Test
    public void createUpdateReadDeleteFileWithFSDataStreamsChars() throws IOException {
        Path testFilePath = new Path(TestConfig.HDFS_DIRECTORY, "test.txt");
        String line = "The cow is of the bovine ilk; one end is moo, the other milk.";
        writeStringWithFSDataOutputStreamChars(testFilePath, line);
        assertThat(fileSystem.isFile(testFilePath)).isTrue();

        String string = readTextFileWithFSDataInputStreamChars(testFilePath);
        assertThat(string).isEqualTo(line);

        fileSystem.delete(testFilePath, false);
        assertThat(fileSystem.isFile(testFilePath)).isFalse();
    }

    private void writeStringWithFSDataOutputStreamChars(Path testFilePath, String line) throws IOException {
        try(FSDataOutputStream fsDataOutputStream = fileSystem.create(testFilePath)) {
            fsDataOutputStream.writeChars(line);
        }
    }

    private String readTextFileWithFSDataInputStreamChars(Path testFilePath) throws IOException {
        StringBuilder stringBuffer = new StringBuilder();
        try(FSDataInputStream fsDataInputStream = fileSystem.open(testFilePath)) {
            while (fsDataInputStream.available() > 0) {
                char ch = fsDataInputStream.readChar();
                stringBuffer.append(ch);
            }
        }
        return stringBuffer.toString();
    }

    @Test
    public void createUpdateReadDeleteFileBufferedForString() throws IOException {
        Path testFilePath = new Path(TestConfig.HDFS_DIRECTORY, "test.txt");
        String line = "The cow is of the bovine ilk;\r\none end is moo, the other milk.";
        writeStringBuffered(testFilePath, line);
        assertThat(fileSystem.isFile(testFilePath)).isTrue();

        String string = readStringBuffered(testFilePath);
        assertThat(string).isEqualTo(line);

        fileSystem.delete(testFilePath, false);
        assertThat(fileSystem.isFile(testFilePath)).isFalse();
    }

    private void writeStringBuffered(Path testFilePath, String line) throws IOException {
        try (FSDataOutputStream fsDataOutputStream = fileSystem.create(testFilePath);
            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream))) {
            bufferedWriter.write(line);
        }
    }

    private String readStringBuffered(Path testFilePath) throws IOException {
        try(FSDataInputStream fsDataInputStream = fileSystem.open(testFilePath);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream))) {
            return bufferedReader.lines().collect(Collectors.joining("\r\n"));
        }
    }

    @Test
    public void createUpdateReadDeleteFileBufferedForStringList() throws IOException {
        Path testFilePath = new Path(TestConfig.HDFS_DIRECTORY, "test.txt");
        List<String> lines = Arrays.asList(
                "The cow is of the bovine ilk;",
                "one end is moo, the other milk."
        );
        writeLinesBuffered(testFilePath, lines);
        assertThat(fileSystem.isFile(testFilePath)).isTrue();

        List<String> readLines = readLinesBuffered(testFilePath);
        assertThat(readLines).isEqualTo(lines);

        fileSystem.delete(testFilePath, false);
        assertThat(fileSystem.isFile(testFilePath)).isFalse();
    }

    private void writeLinesBuffered(Path testFilePath, List<String> lines) throws IOException {
        try (FSDataOutputStream fsDataOutputStream = fileSystem.create(testFilePath);
            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream))) {
            lines.stream().forEach(line -> {
                        try {
                            bufferedWriter.write(line + "\n");
                        }
                        catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }
            );
        }
    }

    private List<String> readLinesBuffered(Path testFilePath) throws IOException {
        try(FSDataInputStream fsDataInputStream = fileSystem.open(testFilePath);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream))) {
            return bufferedReader.lines().collect(Collectors.toList());
        }
    }

}
