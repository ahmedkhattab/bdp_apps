package com.hdfs.client;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;

public class HdfsClient {

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

									 
					 
					    final ZipInputStream in = new ZipInputStream(new FileInputStream(args [0]));
					    OutputStream out = fs.create(new Path("/user/hdfs/pagerank"));


					// Get configuration of Hadoop system
					Configuration conf = new Configuration();
					System.out.println("Connecting to -- " + conf.get("fs.defaultFS"));

					// Copy file from local to HDFS
					IOUtils.copyBytes(in, out, 4096, true);
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
}