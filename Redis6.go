package db

import (
	"context"
	"errors"

	"github.com/go-redis/redis/v8"
)

/*
Redis的主从模式，是最原始的集群模式了，基本没啥用。

1，主数据库可以进行读写操作，当读写操作导致数据变化时会自动将数据同步给从数据库
2，从数据库一般都是只读的，并且接收主数据库同步过来的数据
3，一个master可以拥有多个slave，但是一个slave只能对应一个master
4，slave挂了不影响其他slave的读和master的读和写，重新启动后会将数据从master同步过来
5，master挂了以后，不影响slave的读，但redis不再提供写服务，master重启后redis将重新对外提供写服务
6，master挂了以后，不会在slave节点中重新选一个master

工作机制：
1，当slave启动后，主动向master发送SYNC命令。
2，master接收到SYNC命令后在后台保存快照（RDB持久化），并缓存保存快照这段时间的命令。
3，然后将保存的快照文件和缓存的命令发送给slave。
4，slave接收到快照文件和命令后加载快照文件和缓存的执行命令。
5，复制初始化后，master每次接收到的写命令都会同步发送给slave，保证主从数据一致性。

安全设置：
1，当master节点设置密码后，客户端访问master需要密码
2，启动slave需要密码，在配置文件中配置即可
3，客户端访问slave不需要密码
*/

/*
Redis的Sentinel模式，基本能满足一般的应用需求。

1，sentinel模式是建立在主从模式的基础上，如果只有一个Redis节点，sentinel就没有任何意义
2，当master挂了以后，sentinel会在slave中选择一个做为master，并修改它们的配置文件，其他slave的配置文件也会被修改，比如slaveof属性会指向新的master
3，当master重新启动后，它将不再是master而是做为slave接收新的master的同步数据
4，sentinel因为也是一个进程，有挂掉的可能，所以sentinel也会启动多个形成一个sentinel集群
5，多sentinel配置的时候，sentinel之间也会自动监控
6，当主从模式配置密码时，sentinel也会同步将配置信息修改到配置文件中，不需要担心
7，一个sentinel或sentinel集群可以管理多个主从Redis，多个sentinel也可以监控同一个redis
8，sentinel最好不要和Redis部署在同一台机器，不然Redis的服务器挂了以后，sentinel也挂了

工作机制：
1，每个sentinel以每秒钟一次的频率向它所知的master，slave以及其他sentinel实例发送一个 PING 命令
2，如果一个实例距离最后一次有效回复 PING 命令的时间超过 down-after-milliseconds 选项所指定的值，则这个实例会被sentinel标记为主观下线。
3，如果一个master被标记为主观下线，则正在监视这个master的所有sentinel要以每秒一次的频率确认master的确进入了主观下线状态
4，当有足够数量的sentinel（大于等于配置文件指定的值）在指定的时间范围内确认master的确进入了主观下线状态，则master会被标记为客观下线
5，在一般情况下，每个sentinel会以每 10 秒一次的频率向它已知的所有master，slave发送 INFO 命令
6，当master被sentinel标记为客观下线时，sentinel向下线的master的所有slave发送 INFO 命令的频率会从 10 秒一次改为 1 秒一次
7，若没有足够数量的sentinel同意master已经下线，master的客观下线状态就会被移除；
8，若master重新向sentinel的 PING 命令返回有效回复，master的主观下线状态就会被移除

1，客户端就不直接连接Redis，而是连接sentinel，由sentinel来提供具体的可提供服务的Redis实例
2，当master节点挂掉以后，sentinel就会感知并将新的master节点提供给使用者

*/

/*
Sentinel模式基本可以满足一般生产的需求，具备高可用性。
但是当数据量过大到一台服务器存放不下的情况时，主从模式或Redis模式就不能满足需求了。
这个时候需要对存储的数据进行分片，将数据存储到多个Redis实例中。
cluster模式的出现就是为了解决单机Redis容量有限的问题，将Redis的数据根据一定的规则分配到多台机器。

cluster可以说是Redis和主从模式的结合体。
通过cluster可以实现主从和master重选功能。
所以如果配置两个副本三个分片的话，就需要六个Redis实例。
因为Redis的数据是根据一定规则分配到cluster的不同机器的，当数据量过大时，可以新增机器进行扩容。

使用集群，只需要将redis配置文件中的cluster-enable配置打开即可。
每个集群中至少需要三个主数据库才能正常运行，新增节点非常方便。

cluster集群特点：
1，多个redis节点网络互联，数据共享
2，所有的节点都是一主一从（也可以是一主多从），其中从不提供服务，仅作为备用
3，不支持同时处理多个key（如MSET/MGET），因为redis需要把key均匀分布在各个节点上，
并发量很高的情况下同时创建key-value会降低性能并导致不可预测的行为
4，支持在线增加、删除节点
5，客户端可以连接任何一个主节点进行读写
*/

/*
GetSentinelClient comment

	redis.Options{
		Network: "tcp",
		Addr:    "127.0.0.1:26379",
	}
*/
func GetSentinelClient(option *redis.Options) *redis.SentinelClient {
	return redis.NewSentinelClient(option)
}

// PingSentinelClient comment
func PingSentinelClient(client *redis.SentinelClient) error {
	cmd := client.Ping(context.Background())
	_, err := cmd.Result()
	return err
}

// GetMasterAddress comment
func GetMasterAddress(client *redis.SentinelClient, masterName string) (string, error) {
	ctx := context.Background()
	masterInfo, err := client.GetMasterAddrByName(ctx, masterName).Result()
	if err != nil {
		return "", err
	}
	return masterInfo[0] + ":" + masterInfo[1], nil
}

/*
===========================================================
copy from gogstash: inputredis.go
*/
type Redis6 struct {
	Host     string `json:"Host"`     // host:port, default: "localhost:6379"
	DB       int    `json:"DB"`       // redis db, default: 0
	Password string `json:"Password"` // default: ""
	Key      string `json:"Key"`      // where to get data
	PoolSize int    `json:"PoolSize"` // maximum number of socket connections, default: 10

	client *redis.Client
}

func (r *Redis6) initClient() error {
	client := redis.NewClient(&redis.Options{
		Addr:     r.Host,
		DB:       r.DB,
		Password: r.Password,
		PoolSize: r.PoolSize,
	})
	ctx := context.Background()
	client = client.WithContext(ctx)

	if _, err := client.Ping(ctx).Result(); err != nil {
		return errors.New("ping failed")
	}
	r.client = client
	return nil
}

func (r *Redis6) Client() (*redis.Client, error) {
	if r.client == nil {
		err := r.initClient()
		if err != nil {
			return nil, err
		}
	}
	return r.client, nil
}

func (r *Redis6) Close() {
	if r.client != nil {
		r.client.Close()
	}
}
