package gdut.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Scanner;

/**
 * HDFS操作类
 * todo: create cat
 */
public class HdfsUtil {

    private static Scanner scanner = new Scanner(System.in);

    private static FileSystem fs;
    private static Configuration conf = new Configuration();
    private static String hdfs = getHdfsFromXML();
    private static String user = "jjz";

    private static final String[] COMMAND = new String[]{"MKDIR", "RM", "RENAME", "LS", "CREATE", "UPLOAD", "DOWNLOAD", "CAT"};

    public static void connect() throws IOException, InterruptedException {
        System.out.println("Input the HDFS'uri want to connect: ");
        String def = scanner.nextLine();
        if (def.equals("")) {
            def = "119.23.43.240:9000";
        }
        hdfs = "hdfs://" + def;
        System.out.println("Input the username: ");
        user = scanner.nextLine();
        fs = FileSystem.get(URI.create(hdfs), conf, user);
        System.out.println("Connect to " + hdfs + " @" + user);
    }

    public static String getHdfsFromXML() {
        NodeList configuration = null;
        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();

            Document parse = db.parse(HdfsUtil.class.getResourceAsStream("/core-site.xml"));;
            configuration = parse.getElementsByTagName("configuration");

        } catch (SAXException | ParserConfigurationException | IOException e) {
            e.printStackTrace();
        }

        return configuration.item(0).getChildNodes().item(1).getChildNodes().item(3).getFirstChild().getNodeValue();
    }

    public static String[] getPath(String inPath) {
        String[] ret = null;
        try {
            if (fs == null) {
                fs = FileSystem.get(URI.create(hdfs), conf, user);
            }
            FileStatus[] fileStatuses = fs.globStatus(new Path(inPath));

            ret = new String[fileStatuses.length];
            for (int i=0; i<fileStatuses.length; i++) {
                Path path = fileStatuses[i].getPath();
                ret[i] = path.toString();
            }

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        return ret;
    }


    /**
     * 创建文件夹
     * @param folder 文件夹名
     */
    public static void mkdirs(String folder) throws IOException, InterruptedException {
        Path path = new Path(folder);
        if (fs == null) {
            fs = FileSystem.get(URI.create(hdfs), conf, user);
        }
        if (!fs.exists(path)) {
            fs.mkdirs(path);
            System.out.println("Create: " + folder);
        }
    }

    /**
     * 删除文件夹
     * @param folder 文件夹名
     */
    public static void rmr(String folder) throws IOException, InterruptedException {
        Path path = new Path(folder);
        if (fs == null) {
            fs = FileSystem.get(URI.create(hdfs), conf, user);
        }
        if (fs.delete(path, true)) {
            System.out.println("Delete: " + folder);
        } else {
            System.out.println("Error delete!");
        }
    }

    /**
     * 重命名文件
     * @param src 源文件名
     * @param dst 目标文件名
     * */
    public static void rename(String src, String dst) throws IOException, InterruptedException {
        Path name1 = new Path(src);
        Path name2 = new Path(dst);
        if (fs == null) {
            fs = FileSystem.get(URI.create(hdfs), conf, user);
        }
        fs.rename(name1, name2);
        System.out.println("Rename: from " + src + " to " + dst);
    }

    /**
     * 列出该路径的文件信息
     * @param folder 文件夹名
     */
    public static void ls(String folder) throws IOException, InterruptedException {
        Path path = new Path(folder);
        if (fs == null) {
            fs = FileSystem.get(URI.create(hdfs), conf, user);
        }
        FileStatus[] list = fs.listStatus(path);
        System.out.println("==========================================================");
        for (FileStatus f : list) {
            System.out.printf("name: %s, isFolder: %s, size: %d\n", f.getPath().getName(), f.isDirectory(), f.getLen());
        }
        System.out.println("==========================================================");
    }

    /**
     * 创建文件
     * @param file    文件名
     * @param content 文件内容
     */
    public static void createFile(String file, String content) throws IOException, InterruptedException {
        if (fs == null) {
            fs = FileSystem.get(URI.create(hdfs), conf, user);
        }
        byte[] buff = content.getBytes();
        FSDataOutputStream os = null;
        try {
            os = fs.create(new Path(file));
            os.write(buff, 0, buff.length);
            System.out.println("Create: " + file);
        } finally {
            if (os != null)
                os.close();
        }
    }

    /**
     * 复制本地文件到hdfs
     * @param local  本地文件路径
     * @param remote hdfs目标路径
     */
    public static void upload(String local, String remote) throws IOException, InterruptedException {
        if (fs == null) {
            fs = FileSystem.get(URI.create(hdfs), conf, user);
        }
        fs.copyFromLocalFile(new Path(local), new Path(remote));
        System.out.println("copy from: " + local + " to " + remote);
    }

    /**
     * 从hdfs下载文件到本地
     * @param remote hdfs文件路径
     * @param local  本地目标路径
     */
    public static void download(String remote, String local) throws IOException, InterruptedException {
        Path path = new Path(remote);
        if (fs == null) {
            fs = FileSystem.get(URI.create(hdfs), conf, user);
        }
        fs.copyToLocalFile(path, new Path(local));
        System.out.println("download: from" + remote + " to " + local);

    }

    /**
     * 查看hdfs文件内容
     * @param remoteFile hdfs文件路径
     */
    public static void cat(String remoteFile) throws IOException, InterruptedException {
        Path path = new Path(remoteFile);
        if (fs == null) {
            fs = FileSystem.get(URI.create(hdfs), conf, user);
        }
        FSDataInputStream fsdis = null;
        try {
            fsdis = fs.open(path);
            IOUtils.copyBytes(fsdis, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(fsdis);
            fs.close();
        }
    }

    public static void shell() throws IOException, InterruptedException {
        HashSet<String> commandSet = new HashSet<>();
        commandSet.addAll(Arrays.asList(COMMAND));
        connect();

        try {
            while (!scanner.hasNext("exit")) {
                String[] split = scanner.nextLine().split(" ");
                String cmd = split[0].toUpperCase();
                if (commandSet.contains(cmd)) {
                    switch (cmd) {
                        case "MKDIR":
                            mkdirs(split[1]);
                            break;
                        case "RM":
                            rmr(split[1]);
                            break;
                        case "RENAME":
                            rename(split[1], split[2]);
                            break;
                        case "LS":
                            ls(split[1]);
                            break;
                        case "CREATE":
                            createFile(split[1], split[2]);
                            break;
                        case "UPLOAD":
                            upload(split[1], split[2]);
                            break;
                        case "DOWNLOAD":
                            download(split[1], split[2]);
                            break;
                        case "CAT":
                            cat(split[1]);
                            break;
                        default:
                            break;
                    }
                } else {
                    System.out.println("Unknow cmd.");
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fs != null) {
                fs.close();
            }
        }
    }



}
