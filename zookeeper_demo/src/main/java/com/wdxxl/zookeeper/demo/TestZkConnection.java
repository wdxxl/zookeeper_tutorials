package com.wdxxl.zookeeper.demo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

public class TestZkConnection {
	private static ZooKeeper zk;
	private static ZkConnector zkc;
	private static List<String> znodeList = new ArrayList<String>();

	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		zkc = new ZkConnector();
		zk = zkc.connect("localhost");
		znodeList = zk.getChildren("/", true);
		for (String znode : znodeList) {
			System.out.println(znode);
		}
	}
}
