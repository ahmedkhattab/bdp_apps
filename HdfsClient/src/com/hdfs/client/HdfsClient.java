package com.hdfs.client;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

public class HdfsClient {  
	private static final int BUFFER_SIZE = 4096;

	public static void main(final String args[]) {

		try {
			UserGroupInformation ugi = UserGroupInformation.createRemoteUser("hdfs");

			ugi.doAs(new PrivilegedExceptionAction<Void>() {

				public Void run() throws Exception {

					Configuration configuration = new Configuration();
					configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
					configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

					String namenode = String.format("hdfs://%s:8020", System.getenv("NAMENODE_SERVICE_HOST"));

					FileSystem fs = FileSystem.get(new URI(namenode), configuration);
					    
					    String zipFilePath = args [0];
				        String destDirectory = args [0].substring(0, args[0].length()-4);
				        try {
				            unzip(zipFilePath, destDirectory);
				        } catch (Exception ex) {
				            // some errors occurred
				            ex.printStackTrace();
				        }
					  
				        OutputStream outHDFS = fs.create(new Path("/user/hdfs/pagerank"));
					    
					    
					    InputStream inHDFS = new BufferedInputStream(new FileInputStream(destDirectory+"/"+destDirectory));
				        
					// Get configuration of Hadoop system
					Configuration conf = new Configuration();
					System.out.println("Connecting to -- " + conf.get("fs.defaultFS"));

					// Copy file from local to HDFS
					org.apache.hadoop.io.IOUtils.copyBytes(inHDFS, outHDFS, 4096, true);
					FileStatus[] status = fs.listStatus(new Path("/user/hdfs"));
					for (int i = 0; i < status.length; i++) {
						System.out.println(status[i].getPath());
					}
					return null;
				}
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	 public static void unzip(String zipFilePath, String destDirectory) throws IOException {
	        File destDir = new File(destDirectory);
	        if (!destDir.exists()) {
	            destDir.mkdir();
	        }
	        ZipInputStream zipIn = new ZipInputStream(new FileInputStream(zipFilePath));
	        ZipEntry entry = zipIn.getNextEntry();
	        // iterates over entries in the zip file
	        while (entry != null) {
	            String filePath = destDirectory + File.separator + entry.getName();
	            if (!entry.isDirectory()) {
	                // if the entry is a file, extracts it
	                extractFile(zipIn, filePath);
	            } else {
	                // if the entry is a directory, make the directory
	                File dir = new File(filePath);
	                dir.mkdir();
	            }
	            zipIn.closeEntry();
	            entry = zipIn.getNextEntry();
	        }
	        zipIn.close();
	    }
	    /**
	     * Extracts a zip entry (file entry)
	     * @param zipIn
	     * @param filePath
	     * @throws IOException
	     */
	    private static void extractFile(ZipInputStream zipIn, String filePath) throws IOException {
	        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(filePath));
	        byte[] bytesIn = new byte[BUFFER_SIZE];
	        int read = 0;
	        while ((read = zipIn.read(bytesIn)) != -1) {
	            bos.write(bytesIn, 0, read);
	        }
	        bos.close();
	    }
	
}