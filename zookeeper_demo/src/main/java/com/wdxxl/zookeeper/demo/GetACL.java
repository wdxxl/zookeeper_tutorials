package com.wdxxl.zookeeper.demo;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

public class GetACL {
	private static ZooKeeper zk;
	private static ZkConnector zkc;

	public static List<ACL> getacl(String path) throws KeeperException, InterruptedException {
		return zk.getACL(path, zk.exists(path, true));
	}

	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		String path = "/getmyacl";
		zkc = new ZkConnector();
		zk = zkc.connect("localhost");
		List<ACL> acl = getacl(path);
		for (ACL aclitem : acl) {
			System.out.println(aclitem.toString());
		}
	}

}
