# hdfs-demo
This contains a simple application which copies a file into HDFS. It expects HDFS to be running on host = hdfs001.

User Permissions
You will need permission to write to the directory you are using. If you do not have write access, you will see an exception like
```
org.apache.hadoop.security.AccessControlException: Permission denied: user=philippa.main, access=WRITE, inode="/test":hadoop:supergroup:drwxr-xr-x
...
```
To change ownership of a directory, do
```
hadoop fs -chown <user name> <directory path>
```
e.g.
```
hadoop fs -chown philippa.main /user/pm
```
This is only needed until I find out how to connect with a user / group. If anyone knows how to do this, please let me know. Otherwise, watch this space.

A Hadoop Client
You will need to have a Hadoop client JAR that will work with the version of Hadoop you have installed. This application uses 
```'org.apache.hadoop:hadoop-client:2.7.1'```and will work with Hadoop 2.7.1.

From various tutorials I originally looked in the Maven Central repository for the latest version of hadoop-core and found 
```'org.apache.hadoop:hadoop-core:1.2.1'```. This gave me an exception
```
Caused by: org.apache.hadoop.ipc.RemoteException: Server IPC version 9 cannot communicate with client version 4
...
```
so if you get a similar error you may have to hunt around a bit.
