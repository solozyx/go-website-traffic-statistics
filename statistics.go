package main

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/mgutz/str"
	"github.com/sirupsen/logrus"
)

const HANDLE_DIG = " /dig?"
const HANDLE_MOVIE = "/movie/"
const HANDLE_LIST = "/list/"
const HANDLE_HTML = ".html"

// 命令行参数
type cmdParams struct {
	// nginx打点服务器用户访问日志文件
	logFilePath string
	// 协程数量
	routineNum int
}

// 打点数据
type digData struct {
	time  string
	url   string
	refer string
	ua    string
}

type urlData struct {
	data digData
	// 用户id,用于UV统计去重
	uid   string
	unode urlNode
}

// redis持久化数据类型
type urlNode struct {
	// url node 详情页/movie 列表页/list 首页/
	unType string
	// Resource ID 资源ID
	unRid int
	// 当前页面url
	unUrl string
	// 当前访问该页面时间
	unTime string
}

type storageBlock struct {
	// 区分 PV UV
	counterType string
	// 数据存储类型
	storageModel string
	unode        urlNode
}

// 打日志工具
var log = logrus.New()
var redisPool *redis.Pool

// 在程序开始调用
func init() {
	// 日志输出 标准输出
	log.Out = os.Stdout
	// 日志级别 debug最详细
	log.SetLevel(logrus.DebugLevel)
	initRedis()
}

func initRedis() (err error) {
	var (
		pool *redis.Pool
		conn redis.Conn
	)

	pool = &redis.Pool{
		MaxIdle:   64,
		MaxActive: 0,
		// 超时时间 单位 秒 time.Duration 单位是纳秒
		IdleTimeout: time.Duration(300) * time.Second,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", "192.168.174.134:6379")
		},
	}
	conn = pool.Get()

	// 检测Redis连接是否可用
	// TODO : redis 连接失败 主动把程序挂掉
	if _, err := conn.Do("ping"); err != nil {
		fmt.Println("initRedis ping err = ", err)
		panic(err)
	} else {
		redisPool = pool
		go func() {
			for {
				conn.Do("PING")
				time.Sleep(3 * time.Second)
			}
		}()
	}

	defer conn.Close()
	return
}

func main() {
	// 命令行获取参数
	logFilePath := flag.String("logFilePath", "./dig.log", "user access log file path")
	routineNum := flag.Int("routineNum", 5, "log data consumer number of goroutine")
	l := flag.String("l", "./statistics.log", "statistics log file path")
	flag.Parse()

	params := cmdParams{
		*logFilePath,
		*routineNum,
	}

	// 日志
	logFd, err := os.OpenFile(*l, os.O_CREATE|os.O_WRONLY, 0644)
	if err == nil {
		// 标准输出 -> 日志文件输出
		log.Out = logFd
	}
	// 开启资源,有能力在程序中明确资源关闭时刻可以不用defer;不能明确则defer在程序退出时关闭
	defer logFd.Close()

	log.Infof("Exec start.")
	log.Infof("Params: logFilePath=%s, routineNum=%d", params.logFilePath, params.routineNum)

	// 初始化channel,用于数据传递
	// 读取用户访问日志数据并发度大, *3 处理
	var logChannel = make(chan string, 3*params.routineNum)
	var pvChannel = make(chan urlData, params.routineNum)
	var uvChannel = make(chan urlData, params.routineNum)
	var storageChannel = make(chan storageBlock, params.routineNum)

	// 日志文件读取协程
	// 读取日志文件数据写入 logChannel
	go readFileLineByLine(params, logChannel)

	// 日志数据解析协程
	// 从 logChannel读取数据,处理后结果写入 pvChannel uvChannel
	for i := 0; i < params.routineNum; i++ {
		go logConsumer(logChannel, pvChannel, uvChannel)
	}

	// 用户访问 PV 统计协程,消费pvChannel 写入storageChannel
	go pvCounter(pvChannel, storageChannel)
	// 用户访问 UV 统计协程，消费uvChannel 写入storageChannel
	go uvCounter(uvChannel, storageChannel)
	// 可扩展的 xxxCounter

	// 统计数据存储协程
	go dataStorage(storageChannel)

	// 防止main协程退出导致上面子协程退出
	time.Sleep(1000 * time.Second)
}

func dataStorage(storageChannel chan storageBlock) {
	for block := range storageChannel {
		prefix := block.counterType + "_"
		// 逐层添加
		// 维度: 天-小时-分钟
		// 层级: 顶级-大分类-小分类-终极页面
		// 存储模型: Redis SortedSet ZSET
		setKeys := []string{
			// 当前时间所属天 + 1
			prefix + "day_" + getTime(block.unode.unTime, "day"),
			// 当前时间所属小时 + 1
			prefix + "hour_" + getTime(block.unode.unTime, "hour"),
			// 当前时间所属分钟 + 1
			prefix + "min_" + getTime(block.unode.unTime, "min"),
			prefix + block.unode.unType + "_day_" + getTime(block.unode.unTime, "day"),
			prefix + block.unode.unType + "_hour_" + getTime(block.unode.unTime, "hour"),
			prefix + block.unode.unType + "_min_" + getTime(block.unode.unTime, "min"),
		}
		// 对应 /movie/12846.html 的 12846
		rowId := block.unode.unRid
		// 写入redis
		for _, key := range setKeys {
			// ZINCRBY 操作
			// key 集合名
			// 1 增加1
			// rowId 成员名
			_, err := redisPool.Get().Do(block.storageModel, key, 1, rowId)
			if err != nil {
				// 线上日志庞大,写入失败直接放过去
				log.Errorln("dataStorage redis storage error.", block.storageModel, key, rowId)
			}
			// 线上日志庞大,成功写入redis的数据不做日志记录
		}
	}
}

// PV 统计
func pvCounter(pvChannel chan urlData, storageChannel chan storageBlock) {
	for data := range pvChannel {
		sItem := storageBlock{
			counterType: "pv",
			// 使用redis的 sorted set 有序数据集 对应redis操作 ZINCREBY
			storageModel: "ZINCRBY",
			unode:        data.unode,
		}
		storageChannel <- sItem
	}
}

// UV 统计 对同一天用户访问PV去重得到UV
func uvCounter(uvChannel chan urlData, storageChannel chan storageBlock) {
	for data := range uvChannel {
		// TODO - NOTICE : PV去重得到UV,去重行业内使用 HyperLoglog 方法,redis内置 HyperLoglog数据类型
		// redis HyperLoglog
		// 去重要在一定时间范围内去重
		hyperLogLogKey := "uv_hpll_" + getTime(data.data.time, "day")
		// 过期时间 86400 一天
		ret, err := redisPool.Get().Do("PFADD", hyperLogLogKey, data.uid, "EX", 86400)
		if err != nil {
			log.Warningln("uvCounter write redis hyperLogLogKey failed, ", err)
		}
		// 写入redis成功返回 1,不返回1 说明redis已经有该值
		if ret != 1 {
			continue
		}

		sItem := storageBlock{
			counterType:  "uv",
			storageModel: "ZINCRBY",
			unode:        data.unode,
		}
		storageChannel <- sItem
	}
}

func logConsumer(logChannel chan string, pvChannel, uvChannel chan urlData) error {
	for logStr := range logChannel {
		// 切割日志字符串,获取打点上报的数据
		data := cutLogFetchData(logStr)

		// uid
		// TODO : 模拟生成uid, md5(refer+ua) refer和ua是随机生成的,二者有一定几率能碰撞到一起
		hasher := md5.New()
		hasher.Write([]byte(data.refer + data.ua))
		uid := hex.EncodeToString(hasher.Sum(nil))

		// 解析工作 ...

		uData := urlData{
			data:  data,
			uid:   uid,
			unode: formatUrl(data.url, data.time),
		}

		pvChannel <- uData
		uvChannel <- uData
	}
	return nil
}

// 切割日志字符串,获取打点上报的数据
func cutLogFetchData(logStr string) digData {
	// 去掉首尾空白字符
	logStr = strings.TrimSpace(logStr)
	// 该库 对string字符串处理封装比较好
	// 从第0个字符开始找,找到 " /dig?" 第1个出现位置
	pos1 := str.IndexOf(logStr, HANDLE_DIG, 0)
	// 没找到指定字符串
	if pos1 == -1 {
		return digData{}
	}
	pos1 += len(HANDLE_DIG)
	// 这里查找 " HTTP/" 而不是 " HTTP/1.1" 因为有的浏览器是 1.0 有的是 1.1
	// 从 pos1 开始往后找
	pos2 := str.IndexOf(logStr, " HTTP/", pos1)
	// 字符串截取
	d := str.Substr(logStr, pos1, pos2-pos1)

	// 拼接主机前缀,该方法只识别完整网址
	urlInfo, err := url.Parse("http://localhost/?" + d)
	if err != nil {
		return digData{}
	}
	// 调用Parse方法后需要调用Query
	data := urlInfo.Query()
	return digData{
		time:  data.Get("time"),
		refer: data.Get("refer"),
		url:   data.Get("url"),
		ua:    data.Get("ua"),
	}
}

// 逐行消费用户访问日志数据
func readFileLineByLine(params cmdParams, logChannel chan string) error {
	fd, err := os.Open(params.logFilePath)
	if err != nil {
		log.Warningf("readFileLineByLine can't open file:%s", params.logFilePath)
		return err
	}
	defer fd.Close()

	count := 0
	bufferRead := bufio.NewReader(fd)
	for {
		// TODO : 注意这里是 '\n'字符 不是"\n" 字符串
		line, err := bufferRead.ReadString('\n')
		logChannel <- line
		count++
		// 生产日志非常大,读取1行数据就打印1个,量非常大
		if count%(1000*params.routineNum) == 0 {
			log.Infof("readFileLineByLine line: %d", count)
		}
		if err != nil {
			// 日志文件读取完成
			if err == io.EOF {
				time.Sleep(3 * time.Second)
				log.Infof("readFileLineByLine wait, read line:%d", count)
			} else {
				log.Warningf("readFileLineByLine read log file error:%s", err.Error())
			}
		}
	}

	return nil
}

func formatUrl(url, t string) urlNode {
	// 一定从量大的着手,  详情页>列表页≥首页
	pos1 := str.IndexOf(url, HANDLE_MOVIE, 0)
	// 是 /movie/ 详情页
	if pos1 != -1 {
		pos1 += len(HANDLE_MOVIE)
		pos2 := str.IndexOf(url, HANDLE_HTML, 0)
		// domain/movie/1000.html 摘除 1000
		idStr := str.Substr(url, pos1, pos2-pos1)
		id, _ := strconv.Atoi(idStr)
		return urlNode{
			unType: "movie",
			unRid:  id,
			unUrl:  url,
			unTime: t,
		}
	} else {
		pos1 = str.IndexOf(url, HANDLE_LIST, 0)
		// 是 /list/ 列表页
		if pos1 != -1 {
			pos1 += len(HANDLE_LIST)
			pos2 := str.IndexOf(url, HANDLE_HTML, 0)
			idStr := str.Substr(url, pos1, pos2-pos1)
			id, _ := strconv.Atoi(idStr)
			return urlNode{
				unType: "list",
				unRid:  id,
				unUrl:  url,
				unTime: t,
			}
		} else {
			return urlNode{
				unType: "home",
				// 对redis进行写入，认为0是无效数字,首页使用 1
				unRid:  1,
				unUrl:  url,
				unTime: t,
			}
		}
		// 如果页面url有很多种在这里扩展
	}
}

func getTime(logTime, timeType string) string {
	var item string
	switch timeType {
	case "day":
		item = "2006-01-02"
		break
	case "hour":
		item = "2006-01-02 15"
		break
	case "min":
		item = "2006-01-02 15:04"
		break
	}
	// t, _ := time.Parse(item, time.Now().Format(item))
	t, _ := time.Parse(item, logTime)
	// 把64位Unix时间戳转为10进制
	return strconv.FormatInt(t.Unix(), 10)
}
