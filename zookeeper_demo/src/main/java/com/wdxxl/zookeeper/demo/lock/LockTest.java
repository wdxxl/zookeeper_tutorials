package com.wdxxl.zookeeper.demo.lock;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

public class LockTest {
	private String zkQurom = "localhost:2181";
	private String lockNameSapce = "/mylock";
	private String nodeString = lockNameSapce + "/test1";
	private ZooKeeper zk;

	public LockTest() {
		try {
			zk = new ZooKeeper(zkQurom, 6000, new Watcher() {
				@Override
				public void process(WatchedEvent watchedEvent) {
					System.out.println("Receive event " + watchedEvent);
					if (Event.KeeperState.SyncConnected == watchedEvent.getState()) {
						System.out.println("connection is established.");
					}
				}
			});
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void ensureRootPath() {
		try {
			if (zk.exists(lockNameSapce, true) == null) {
				zk.create(lockNameSapce, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	private void watchNode(String nodeString, final Thread thread) {
		try {
			zk.exists(nodeString, new Watcher() {
				@Override
				public void process(WatchedEvent watchedEvent) {
					System.out.println("==" + watchedEvent.toString());
					if (watchedEvent.getType() == Event.EventType.NodeDeleted) {
						System.out.println("Threre is a Thread released Lock 1==============");
						thread.interrupt();
					}
					try {
						zk.exists(nodeString, new Watcher() {
							@Override
							public void process(WatchedEvent watchedEvent) {
								System.out.println("==" + watchedEvent.toString());
								if (watchedEvent.getType() == Event.EventType.NodeDeleted) {
									System.out.println("Threre is a Thread released Lock 2==============");
									thread.interrupt();
								}
								try {
									zk.exists(nodeString, true);
								} catch (KeeperException | InterruptedException e) {
									e.printStackTrace();
								}
							}
						});
					} catch (KeeperException | InterruptedException e) {
						e.printStackTrace();
					}
				}
			});
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	public boolean lock() {
		String path = null;
		ensureRootPath();
		watchNode(nodeString, Thread.currentThread());
		while (true) {
			try {
				path = zk.create(nodeString, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			} catch (KeeperException | InterruptedException e) {
				System.out.println(Thread.currentThread().getName() + "  getting Lock but can not get");
				try {
					Thread.sleep(5000);
				} catch (InterruptedException ex) {
					System.out.println("thread is notify");
				}
			}
			if (path != null && path != "") {
				System.out.println(Thread.currentThread().getName() + "  get Lock...");
				return true;
			}
		}
	}

	public void unlock() {
		try {
			System.out.println(Thread.currentThread().getName() + " release lock.");
			zk.delete(nodeString, -1);
		} catch (InterruptedException | KeeperException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		ExecutorService service = Executors.newFixedThreadPool(10);
		for (int i = 0; i < 4; i++) {
			service.execute(() -> {
				LockTest test = new LockTest();
				try {
					test.lock();
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				test.unlock();
			});
		}
		service.shutdown();
	}
}
