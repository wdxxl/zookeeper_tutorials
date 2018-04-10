package com.wdxxl.zookeeper.curator;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreV2;
import org.apache.curator.framework.recipes.locks.Lease;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

// https://www.jianshu.com/p/31335efec309
// ZooKeeper + Curator 实现分布式锁
// https://my.oschina.net/u/237688/blog/808415
public class DistributedLockDemo_semaphore {
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

	// 共享信号量
	@Test
	public void semaphore() throws Exception {
		// 创建一个信号量，Curator以公平锁的方式进行实现
		InterProcessSemaphoreV2 semaphore1 = new InterProcessSemaphoreV2(client1, lockPath, 6);
		// semaphore2 用于模拟其他客户端
		InterProcessSemaphoreV2 semaphore2 = new InterProcessSemaphoreV2(client2, lockPath, 6);

		// 获取一个许可
		Lease lease1 = semaphore1.acquire();
		Assert.assertNotNull(lease1);
		// semaphore1.getParticipantNodes() 会返回当前参与信号量的节点列表，俩个客户端获取的信息相同
		Assert.assertEquals(semaphore1.getParticipantNodes(), semaphore2.getParticipantNodes());

		// 超时获取一个许可
		Lease lease2 = semaphore2.acquire(2, TimeUnit.SECONDS);
		Assert.assertNotNull(lease2);
		Assert.assertEquals(semaphore1.getParticipantNodes(), semaphore2.getParticipantNodes());

		// 获取多个许可，参数为许可数量
		Collection<Lease> leases1 = semaphore1.acquire(2);
		Assert.assertTrue(leases1.size() == 2);
		Assert.assertEquals(semaphore1.getParticipantNodes(), semaphore2.getParticipantNodes());

		// 超时获取多个许可，第一个参数为许可数量
		Collection<Lease> leases2 = semaphore1.acquire(2, 2, TimeUnit.SECONDS);
		Assert.assertTrue(leases2.size() == 2);
		Assert.assertEquals(semaphore1.getParticipantNodes(), semaphore2.getParticipantNodes());

		// 目前 semaphore 已经获取3个许可，semephore2 也获取了3个许可，加起来为6个，所以它们无法再进行许可获取
		Assert.assertNull(semaphore1.acquire(2, TimeUnit.SECONDS));
		Assert.assertNull(semaphore2.acquire(2, TimeUnit.SECONDS));

		// 释放一个许可
		semaphore1.returnLease(lease1);
		semaphore2.returnLease(lease2);
		// 释放多个许可
		semaphore1.returnAll(leases1);
		semaphore2.returnAll(leases2);
	}
}
