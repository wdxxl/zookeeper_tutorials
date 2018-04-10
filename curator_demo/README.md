
```
常见报错:版本问题
org.apache.zookeeper.KeeperException$UnimplementedException: KeeperErrorCode = Unimplemented for /distributed-lock/locks/_c_d3f07bc2-8dc1-4978-808f-6525819696dd-lock-
	at org.apache.zookeeper.KeeperException.create(KeeperException.java:103)
	at org.apache.zookeeper.KeeperException.create(KeeperException.java:51)
	at org.apache.zookeeper.ZooKeeper.create(ZooKeeper.java:1525)

https://my.oschina.net/u/237688/blog/808415

解决方案: compile group: 'org.apache.curator', name: 'curator-recipes', version: '2.12.0'
```