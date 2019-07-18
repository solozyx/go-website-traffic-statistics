package main

import (
	"flag"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// 常见ua
// http://www.veryhuo.com/a/view/36482.html
var uaList = []string{
	// PC端
	// safari 5.1 – MAC
	"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
	// safari 5.1 – Windows
	"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
	// IE 9.0
	"Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0;",
	// IE 8.0
	"Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0)",
	// IE 7.0
	"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0)",
	// IE 6.0
	"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)",
	// Firefox 4.0.1 – MAC
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
	// Firefox 4.0.1 – Windows
	"Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
	// Opera 11.11 – MAC
	"Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11",
	// Opera 11.11 – Windows
	"Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11",
	// Chrome 17.0 – MAC
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
	// 傲游(Maxthon)
	"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Maxthon 2.0)",
	// 腾讯TT
	"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; TencentTraveler 4.0)",
}

type resource struct {
	url    string
	target string
	start  int
	end    int
}

func ruleResource() []resource {
	var res []resource
	// 首页 http://localhost:8888/
	r1 := resource{
		url:    "http://localhost:8888/",
		target: "",
		start:  0,
		end:    0,
	}
	// 列表页 http://localhost:8888/list/1.html
	r2 := resource{
		url:    "http://localhost:8888/list/{$id}.html",
		target: "{$id}",
		start:  1,
		end:    21,
	}
	// 详情页 http://localhost:8888/movie/1.html
	r3 := resource{
		url:    "http://localhost:8888/movie/{$id}.html",
		target: "{$id}",
		start:  0,
		end:    12924,
	}
	res = append(res, r1, r2, r3)
	return res
}

// 构造真实网站url集合
func buildUrl(res []resource) []string {
	var list []string
	// _ 忽略下标
	for _, resItem := range res {
		if len(resItem.target) == 0 {
			list = append(list, resItem.url)
		} else {
			for i := resItem.start; i < resItem.end; i++ {
				// -1 全部替换
				urlStr := strings.Replace(resItem.url, resItem.target, strconv.Itoa(i), -1)
				list = append(list, urlStr)
			}
		}
	}
	return list
}

func makeLog(currentUrl, referUrl, ua string) string {
	// 上报到nginx的用户访问网站打点数据 都做了urlencode
	u := url.Values{}
	u.Set("time", "1")
	u.Set("url", currentUrl)
	u.Set("refer", referUrl)
	u.Set("ua", ua)
	// url encode
	paramsStr := u.Encode()
	// fmt.Printf("paramsStr = %s \n",paramsStr)

	logTemplate := "127.0.0.1 - - [08/Mar/2018:00:48:34 +0800] \"OPTIONS /dig?{$paramsStr} " +
		"HTTP/1.1\" 200 43 \"-\" \"{$ua}\" \"-\""
	log := strings.Replace(logTemplate, "{$paramsStr}", paramsStr, -1)
	log = strings.Replace(log, "{$ua}", ua, -1)
	return log
}

func randInt(min, max int) int {
	// 给定种子避免在同一时间内随机数重复
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	if min > max {
		return max
	}
	return r.Intn(max-min) + min
}

func main() {
	// 接收命令行参数
	// 接收参数名 默认值100 参数说明
	total := flag.Int("total", 10000, "logs rows to create")
	filePath := flag.String("filePath", "./dig.log", "log file path")
	// 解析flag接收参数,使接收命令行参数生效
	flag.Parse()
	fmt.Println(*total)
	fmt.Println(*filePath)

	res := ruleResource()
	list := buildUrl(res)

	logStr := ""
	for i := 0; i < *total; i++ {
		currentUrl := list[randInt(0, len(list)-1)]
		referUrl := list[randInt(0, len(list)-1)]
		ua := uaList[randInt(0, len(uaList)-1)]
		logStr = logStr + makeLog(currentUrl, referUrl, ua) + "\n"
	}
	// 覆盖写
	// ioutil.WriteFile(*filePath,[]byte(logStr),0644)
	// 追加写
	fd, _ := os.OpenFile(*filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	fd.Write([]byte(logStr))
	fd.Close()
	fmt.Println("done :)")
}
