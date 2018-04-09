package com.wdxxl.zookeeper.demo;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

// Getting Authenticated ZNode Data
public class GetACL2 {
	private static ZooKeeper zk;
	private static ZkConnector zkc;

	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		String path = "/getmyacl";
		String user = "datanotfound";
		String pwd = "channel123";
		zkc = new ZkConnector();
		zk = zkc.connect("localhost");
		zk.addAuthInfo("digest", (user + ":" + pwd).getBytes());
		byte[] data = zk.getData(path, true, zk.exists(path, true));

		for (byte b : data) {
			System.out.print((char) b);
		}
	}
}
