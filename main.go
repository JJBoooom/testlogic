package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"runtime"
	"sync"
	"time"

	"math/rand"

	"github.com/Sirupsen/logrus"
)

var (
	clientMap       = make(map[string]*Client) //map[ip]Client
	mMap            = new(sync.RWMutex)
	registerChannel = make(chan Client)
	pushedMap       []string
	pMap            = new(sync.RWMutex)
	actions         = []string{"pull", "push"}
	doneChannel     = make(chan int)
	log             = logrus.New()
)

type Client struct {
	BaseClient
	host       string //<ip>:<port>
	port       string
	state      int
	illChannel chan int
}

type RegistryClient struct {
	Client
}

type imagelist struct {
	images []image `json:"images"`
}

type image struct {
	Name  string `json:"name"`
	tag   string `json:"tag"`
	state int    //1-working, 0 - free
}

type dockerMux struct {
}

func (p *dockerMux) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/register" {
		register(w, r)
		return
	}
	http.NotFound(w, r)
	return
}

func register(w http.ResponseWriter, r *http.Request) {
	ip := "192.168.4.23"
	port := "2"
	var c2 = new(Client)
	c2.host = ip + ":" + port
	c2.Opts.Url = "http://" + c2.host
	c2.Opts.Timeout = 1000 * time.Second
	c2.illChannel = make(chan int)

	//注册新的主机,或者修改旧的主机
	mMap.Lock()
	clientMap[c2.host] = c2
	mMap.Unlock()
	return
}

func (c Client) getImages() error {
	resp, err := c.DoAction("/list", Get)
	if err != nil {
		return err
	}
	defer func() {
		if resp != nil {
			resp.Body.Close()
		}
	}()
	var i image
	byteContent, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	json.Unmarshal(byteContent, &i)
	return nil

}

func (c Client) pullImage(img string) error {
	resp, err := c.DoAction("/list/"+img, Get)
	if err != nil {
		return err
	}
	defer func() {
		if resp != nil {
			resp.Body.Close()
		}
	}()
	var i image
	byteContent, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	json.Unmarshal(byteContent, &i)
	return nil

}

func (c Client) pushImage(img string) error {
	resp, err := c.DoAction("/list/"+img, Get)
	if err != nil {
		return err
	}
	defer func() {
		if resp != nil {
			resp.Body.Close()
		}
	}()

	if resp.StatusCode == http.StatusOK {
		pMap.Lock()
		defer pMap.Unlock()
		pushedMap = append(pushedMap, img)
	} else {
		//record error
	}

	return nil
}

func randTest(client Client) {
	//worker is free
	if client.state == 0 {
		lPushedMap := len(pushedMap)
		if lPushedMap == 0 {
			panic("test without pushed image")
		}

		rand.Seed(time.Now().Unix())
		image := pushedMap[rand.Intn(lPushedMap)]

		rand.Seed(time.Now().Unix())
		k := rand.Intn(len(actions))
		switch actions[k] {
		case "push":
			client.pushImage(image)
		case "pull":
			client.pullImage(image)
		default:
			fmt.Println("invalid action")
		}
	}
}

func healthCheck(c Client) {
	Timeout := time.Duration(1)
	client := &http.Client{Timeout: Timeout}

	req, err := http.NewRequest(Get.Name, c.host+"/ping", nil)
	if err != nil {
		panic(err.Error())
	}

	_, err = client.Do(req)
	if err != nil {
		c.illChannel <- 1
	}
}

func registryHealthCheck(r RegistryClient) {
	Timeout := time.Duration(3)
	client := &http.Client{Timeout: Timeout}

	req, err := http.NewRequest(Get.Name, r.host+"/ping", nil)
	if err != nil {
		panic(err.Error())
	}

	_, err = client.Do(req)
	if err != nil {
		log.Errorf("registry[%s]:healthCheck fail", r.host)
		r.illChannel <- 1
	}
}

func NewRegisryClient() *RegistryClient {
	ip := "192.168"
	port := "5000"
	var c2 = new(RegistryClient)

	c2.host = ip + ":" + port
	opts := ClientOpts{
		Url:     "http://" + c2.host,
		Timeout: 3 * time.Second,
	}
	c2.Opts = &opts
	c2.illChannel = make(chan int)

	return c2

}

func main() {

	mMap.RLock()
	for _, client := range clientMap {
		client.DoAction("/list", Get)
	}
	mMap.RUnlock()

	//将容器镜像拉取到指定的主机
	//启动容器后,向当前服务器注册

	//服务器记录已注册的客户机,未注册的客户机则标记失败.
	//服务器对客户机进行定期健康监测
	//健康监测失败客户机标记失败.
	//维护一条对registry的定期健康监测。

	//提取镜像列表

	//获取各客户机镜像列表
	//已存在的镜像不再拉取，
	//未存在的镜像随机找一台主机拉取

	//将镜像推送到registry中

	//随机挑选一个并没有工作的客户机，随机执行拉取，删除等动作

	//添加新的测试机

	go func() {
		log.Info("start register server")
		mux := &dockerMux{}
		http.ListenAndServe(":12345", mux)
	}()

	//	registry healthy check
	log.Info("start registry client, healthCheck")
	registry := NewRegisryClient()
	go func(r RegistryClient) {
		for {
			registryHealthCheck(r)
			time.Sleep(3 * time.Second)
		}
	}(*registry)

	log.Info("start to run test clients...")
	mMap.RLock()
	for _, client := range clientMap {
		go func(c Client) {
			go healthCheck(c)
			for {
				select {
				case _ = <-c.illChannel:
					mMap.Lock()
					delete(clientMap, c.host)
					mMap.Unlock()
					runtime.Goexit()
				default:
					randTest(c)
				}

			}
		}(*client)
	}
	mMap.RUnlock()
	_ = <-registry.illChannel //阻塞主线程
	fmt.Println("registry healthy check fail, exit...")
}
