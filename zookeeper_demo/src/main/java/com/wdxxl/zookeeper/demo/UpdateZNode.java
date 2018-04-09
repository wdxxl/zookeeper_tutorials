package com.wdxxl.zookeeper.demo;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import com.wdxxl.zookeeper.demo.ZkConnector;

public class UpdateZNode {
	private static ZooKeeper zk;
	private static ZkConnector zkc;

	public static void update(String path, byte[] data) throws KeeperException, InterruptedException {
		zk.setData(path, data, zk.exists(path, true).getVersion());
	}

	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		String path = "/sampleznode";
		byte[] data = "sample znode data verion2".getBytes();
		zkc = new ZkConnector();
		zk = zkc.connect("localhost");
		update(path, data);
	}
}
