package com.wdxxl.zookeeper.curator;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMultiLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

// https://www.jianshu.com/p/31335efec309
// ZooKeeper + Curator 实现分布式锁
// https://my.oschina.net/u/237688/blog/808415
public class DistributedLockDemo_multiLock {
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

	// 多重共享锁
	@Test
	public void multiLock() throws Exception {
		// 可重入锁
		InterProcessLock interProcessLock1 = new InterProcessMutex(client1, lockPath);
		// 不可重入锁
		InterProcessLock interProcessLock2 = new InterProcessSemaphoreMutex(client2, lockPath);
		// 创建多重锁对象
		InterProcessLock lock = new InterProcessMultiLock(Arrays.asList(interProcessLock1, interProcessLock2));
		// 获取参数集合中的所有锁
		lock.acquire();

		// 因为存在一个不可重入锁，所以整个 InterProcessMultiLock 不可重入
		Assert.assertFalse(lock.acquire(2, TimeUnit.SECONDS));
		// interPrcessLock1 是可以重入锁，所以可以继续获取锁
		Assert.assertTrue(interProcessLock1.acquire(2, TimeUnit.SECONDS));
		// interPrcessLock2 是不可重入锁，所以可以继续获取锁失败
		Assert.assertFalse(interProcessLock2.acquire(2, TimeUnit.SECONDS));

		// 释放参数集合中的所有锁
		lock.release();

		// interPrcessLock2 是不可重入锁，已经释放成功，所以可以继续获取锁
		Assert.assertTrue(interProcessLock2.acquire(2, TimeUnit.SECONDS));
	}
}
