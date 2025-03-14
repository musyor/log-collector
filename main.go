package log_collector

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/fsnotify/fsnotify"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
)

type logEntry struct {
	TimeStamp string
	Level     string
	Message   string
}

// 定义变量参数 ---> 判断文件是否存在  ---> 构建多协程  ---> 监听/解析/发送一体  ---> 监听系统中断信号，关闭采集器
func main() {
	//创建变量
	logPath := "./test.log"
	esUrl := []string{"http://localhost:9200"}
	username := "elastic"
	password := "elastic"
	indexName := "test"

	//创建日志文件
	if _, err := os.Stat(logPath); os.IsExist(err) {
		file, err := os.Create(logPath)
		if err != nil {
			log.Fatalf("日志文件创建失败：%s", err)
		}
		file.Close()
		log.Println("日志文件创建成功...")
	}
	//创建管道
	logLine := make(chan string, 100)
	logEntries := make(chan logEntry, 100)

	//创建waitGroup
	var wg *sync.WaitGroup
	wg.Add(3)

	//创建协程
	go fileWatcher(logPath, logLine, wg)
	log.Println("文件监听器已启动")
	go logParse(logLine, logEntries, wg)
	log.Println("日志解析器已启动")
	go esWriter(logEntries, esUrl, indexName, username, password, wg)
	log.Println("ES写入器已启动")

	// 创建一个简单的退出机制
	fmt.Println("日志采集器已启动。按Ctrl+C退出...")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c

	wg.Wait()
	fmt.Println("日志采集器关闭...")
}

// 文件监听器
func fileWatcher(filepath string, logLine chan<- string, wg *sync.WaitGroup) {
	//创建监听器
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return
	}
	defer watcher.Close()

	//监听文件
	err = watcher.Add(filepath)
	if err != nil {
		return
	}

	//打开文件
	file, err := os.Open(filepath)
	if err != nil {
		return
	}

	//切换至文件末尾
	_, err = file.Seek(0, 2)
	if err != nil {
		return
	}

	//开启扫描
	scan := bufio.NewScanner(file)

	for {
		select {
		//监听到事件发送
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}
			//监听到写入文件
			if event.Op&fsnotify.Write == fsnotify.Write {
				//逐行扫描
				for scan.Scan() {
					//将每一行写入到log chan中
					line := scan.Text()
					logLine <- line
				}
				if err := scan.Err(); err != nil {
					log.Fatalf("日志扫描错误：%s", err)
				}
			}
			//监听到文件错误
		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			log.Fatalf("文件监听异常：%s", err)
		}
	}
}

// 判断是否包含时间字段  --->  提取时间  ---> 提取事件等级 ---> 提取事件详情  ---> 将事件发送到entry管道
// 假设日志格式为: 2023-10-25 12:34:56 [INFO] This is a log message
func logParse(logLine <-chan string, logEntries chan<- logEntry, wg *sync.WaitGroup) {
	defer wg.Done()
	//循环提取日志信息
	for line := range logLine {
		var entry logEntry
		//若数量>20 符合上面情况
		if len(line) > 20 {
			//提取时间字段
			entry.TimeStamp = line[0:19]
			startLevel := -1
			endLevel := -1
			//判断后续的日志等级  日志详情
			for i := 20; i < len(line); i++ {
				//左边界  因为[0,20]包含了 line[0]到lin[19]的内容 因此左边界需要+1   形成后续的结果不包含[
				if line[i] == '[' && startLevel == -1 {
					startLevel = i + 1
				} else if line[i] == ']' && endLevel == -1 {
					//右边界   不需要-1 不包含右边界
					endLevel = i
					break
				}
			}
			//找到左右边界  写入日志信息
			if startLevel != -1 && endLevel != -1 {
				entry.Level = line[startLevel:endLevel]
				entry.Message = line[endLevel+1:]
			}
			//发送日志信息
			logEntries <- entry
		}
	}
}

func esWriter(logEntries <-chan logEntry, esUrl []string, indexName string, username string, password string, wg *sync.WaitGroup) {
	defer wg.Done()

	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: esUrl,
		Username:  username,
		Password:  password,
	})
	if err != nil {
		log.Fatalf("es打开失败：%s", err)
	}

	resp, err := client.Info()
	if err != nil {
		return
	}
	defer resp.Body.Close()

	for entry := range logEntries {
		jsonData, err := json.Marshal(entry)
		if err != nil {
			return
		}
		res, err := client.Index(indexName, strings.NewReader(string(jsonData)))
		if err != nil {
			return
		}
		res.Body.Close()
		log.Printf("写入日志成功：%s", entry.Message)
	}
}
