package com.wdxxl.zookeeper.demo;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

public class DeleteZNode {
	private static ZooKeeper zk;
	private static ZkConnector zkc;

	public static void delete(String path) throws KeeperException, InterruptedException {
		zk.delete(path, zk.exists(path, true).getVersion());
	}

	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		String path = "/sampleznode";
		zkc = new ZkConnector();
		zk = zkc.connect("localhost");
		delete(path);
	}
}
