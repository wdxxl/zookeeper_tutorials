package com.wdxxl.zookeeper.curator;

import java.util.concurrent.TimeUnit;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
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
public class DistributedLockDemo_sharedLock {
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

	// 共享锁
	@Test
	public void sharedLock() throws Exception {
		// 创建共享锁
		InterProcessLock lock1 = new InterProcessSemaphoreMutex(client1, lockPath);
		// lock2 用于模拟其他客户端
		InterProcessLock lock2 = new InterProcessSemaphoreMutex(client2, lockPath);
		// 获取锁对象
		lock1.acquire();
		// 测试是否可以重入
		// 超时获取锁对象（第一个参数为时间，第二个参数为时间单位），因为已经被获取，所以返回false
		Assert.assertFalse(lock1.acquire(2, TimeUnit.SECONDS));
		// 释放锁
		lock1.release();
		// lock2 尝试获取锁成功，因为所以就被释放
		Assert.assertTrue(lock2.acquire(2, TimeUnit.SECONDS));
		lock2.release();
	}
}
