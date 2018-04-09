package com.wdxxl.zookeeper.demo.watcher;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;

public class ZookeeperWatcher implements Runnable {
	public static final String PATH = "/test";// 所要监控的结点
	private static ZooKeeper zk;
	private static List<String> nodeList;// 所要监控的结点的子结点列表

	public static void main(String[] args) throws IOException {
		zk = new ZooKeeper("localhost:2181", 5000, new Watcher() {
			public void process(WatchedEvent event) {
			}
		});
		ZookeeperWatcher watcher = new ZookeeperWatcher();
		Thread th = new Thread(watcher);
		th.start();
	}

	public void run() {
		Watcher watcher = new Watcher() {
			public void process(WatchedEvent event) {
				System.out.println("step into watcher.");
				// 主节点的数据发生变化时
				if (event.getType() == EventType.NodeDataChanged) {
					System.out.println("Node data changed: " + event.getPath());
				}
				if (event.getType() == EventType.NodeDeleted) {
					System.out.println("Node deleted: " + event.getPath());
				}
				if (event.getType() == EventType.NodeCreated) {
					System.out.println("Node created: " + event.getPath());
				}
				// 获取更新后的nodeList
				try {
					nodeList = zk.getChildren(event.getPath(), false);
				} catch (KeeperException e) {
					System.out.println(event.getPath() + " has no child, deleted");
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		};

		while (true) {
			try {
				zk.exists(PATH, watcher); // 所有要监控的主节点
				nodeList = zk.getChildren(PATH, watcher);
				for (String nodeName : nodeList) {
					try {
						zk.exists(PATH + "/" + nodeName, watcher);
					} catch (KeeperException | InterruptedException e) {
						e.printStackTrace();
					}
				}
				Thread.sleep(5000);
			} catch (KeeperException | InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

}
