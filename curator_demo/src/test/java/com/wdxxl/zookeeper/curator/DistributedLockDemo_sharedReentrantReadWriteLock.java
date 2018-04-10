package com.wdxxl.zookeeper.curator;

import java.util.HashSet;
import java.util.Set;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

// https://www.jianshu.com/p/31335efec309
// ZooKeeper + Curator 实现分布式锁
// https://my.oschina.net/u/237688/blog/808415
public class DistributedLockDemo_sharedReentrantReadWriteLock {
	// ZooKeeper 锁节点路径，分布式锁的相关操作都是在这个节点上进行
	private final String lockPath = "/distributed-lock";
	// ZooKeeper 服务地址, 单机格式为:(127.0.0.1:2181),
	// 集群格式为:(127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183)
	private String connectString;
	// Curator 客户端重试策略
	private RetryPolicy retry;
	// Curator 客户端对象
	private CuratorFramework client1;
	// Curator 用户模拟其他客户端
	private CuratorFramework client2;

	// 初始化资源
	@Before
	public void init() {
		connectString = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";
		// 重试策略：初始休眠为 1000ms, 最大重试次数为 3
		retry = new ExponentialBackoffRetry(1000, 3);
		// 创建一个客户端，60000(ms)为session超时时间, 15000(ms)为连接超时时间
		client1 = CuratorFrameworkFactory.newClient(connectString, 60000, 15000, retry);
		client2 = CuratorFrameworkFactory.newClient(connectString, 60000, 15000, retry);
		// 创建会话
		client1.start();
		client2.start();
	}

	// 释放资源
	@After
	public void close() {
		CloseableUtils.closeQuietly(client1);
		CloseableUtils.closeQuietly(client2);
	}

	// 共享可重入读写锁
	@Test
	public void sharedReentrantReadWriteLock() throws InterruptedException {
		// 创建读写锁对象，Curator 以公平锁的方式实现
		InterProcessReadWriteLock lock1 = new InterProcessReadWriteLock(client1, lockPath);
		// lock2 用于模拟其他客户端
		InterProcessReadWriteLock lock2 = new InterProcessReadWriteLock(client2, lockPath);
		// 使用 lock1 模拟读操作
		// 使用 lock2 模拟写操作
		// 获取读锁 (使用 InterProcessMutex 实现，所以是可以重入的)
		InterProcessLock readLock = lock1.readLock();
		// 获取写锁 (使用 InterProcessMutex 实现，所以是可以重入的)
		InterProcessLock writeLock = lock2.writeLock();

		/** 读写锁测试对象 **/
		class ReadWriteLockTest {
			// 测试数据变更字段
			private Integer testData = 0;
			private Set<Thread> threadSet = new HashSet<>();

			// 写入数据
			private void write() throws Exception {
				writeLock.acquire();
				try {
					Thread.sleep(10);
					testData++;
					System.out.println("写入数据 \t " + testData);
				} finally {
					writeLock.release();
				}
			}

			// 读取数据
			private void read() throws Exception {
				readLock.acquire();
				try {
					Thread.sleep(10);
					System.out.println("读取数据 \t " + testData);
				} finally {
					readLock.release();
				}
			}

			// 等待线程结束，防止test方法调用完成后，当前线程直接退出，导致后台无法输出信息
			public void waitThread() throws InterruptedException {
				for (Thread thread : threadSet) {
					thread.join();
				}
			}

			// 创建线程方法
			private void createThread(int type) {
				Thread thread = new Thread(new Runnable() {
					@Override
					public void run() {
						try {
							if (type == 1) {
								write();
							} else {
								read();
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				});
				threadSet.add(thread);
				thread.start();
			}

			// 测试方法
			public void test() {
				for (int i = 0; i < 5; i++) {
					createThread(1);
				}
				for (int i = 0; i < 5; i++) {
					createThread(2);
				}
			}
		}

		ReadWriteLockTest readWriteLockTest = new ReadWriteLockTest();
		readWriteLockTest.test();
		readWriteLockTest.waitThread();
	}
}
