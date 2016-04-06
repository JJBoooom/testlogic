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
	"golang.org/x/crypto/ssh"
)

const (
	pushed  = 4
	pushing = 3
	pulled  = 2
	pulling = 1
	unpull  = 0

	working = 1
	free    = 0
)

var (
	clientMap       = make(map[string]*Client) //map[ip]Client
	mMap            = new(sync.RWMutex)
	registerChannel = make(chan Client)
	pushedMap       = make(map[string]int)
	pMap            = new(sync.RWMutex)
	actions         = []string{"pull", "push"}
	doneChannel     = make(chan int)
	log             = logrus.New()
	HostJson        = "./host.json"
	ImageJson       = "./image.json"
	imageMap        = make(map[string]*image)
	iMap            = new(sync.RWMutex)
	appSoarRegistry = "192.168.15.119:5000"
)

type Clients struct {
	Hosts []Client `json:"hosts"`
}

type Client struct {
	BaseClient
	Ip         string `json:"ip"`
	Port       string `json:"port"`
	state      int    //0 - free, 1 - working
	illChannel chan int
	User       string `json:"user"`
	Password   string `json:"password"`
}

type RegistryClient struct {
	Client
}

type imagelist struct {
	images []image `json:"images"`
}

type image struct {
	Name  string `json:"name"`
	Tag   string `json:"tag"`
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
	c2.Ip = ip + ":" + port
	c2.Opts.Url = "http://" + c2.Ip + ":" + c2.Port
	c2.Opts.Timeout = 1000 * time.Second
	c2.illChannel = make(chan int)

	//注册新的主机,或者修改旧的主机
	mMap.Lock()
	clientMap[c2.Ip+":"+c2.Port] = c2
	mMap.Unlock()
	//新注册的client开始随机测试
	go func(c Client) {
		go healthCheck(c)
		for {
			select {
			case _ = <-c.illChannel:
				mMap.Lock()
				delete(clientMap, c.Ip+":"+c.Port)
				mMap.Unlock()
				runtime.Goexit()
			default:
				randTest(c)
			}
		}
	}(*c2)
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

func (c Client) deploy() (err error) {

	command := "docker pull testAgent"

	config := &ssh.ClientConfig{
		User: c.User,
		Auth: []ssh.AuthMethod{ssh.Password(c.Password)},
	}

	conn, err := ssh.Dial("tcp", c.Ip+":"+c.Port, config)
	if err != nil {
		log.Debugf("can't connect to remote host, %s", err)
		return
	}
	defer conn.Close()

	session, err := conn.NewSession()
	if err != nil {
		log.Debugf("unable to create session: %s", err)
		return
	}
	defer session.Close()

	err = session.Run(command)
	return
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
		//pushedMap = append(pushedMap, img)
		pMap.Lock()
		pushedMap[img] = 1
		pMap.Unlock()
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
		//image := pushedMap[rand.Intn(lPushedMap)]
		iList := make([]string, lPushedMap)
		i := 0
		for j, _ := range pushedMap {
			iList[i] = j
			i++
		}
		image := iList[rand.Intn(len(iList))]

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
	Timeout := time.Duration(3 * time.Second)
	client := &http.Client{Timeout: Timeout}

	req, err := http.NewRequest(Get.Name, "http://"+c.Ip+":"+c.Port+"/ping", nil)
	if err != nil {
		panic(err.Error())
	}

	_, err = client.Do(req)
	if err != nil {
		c.illChannel <- 1
	}
}

func registryHealthCheck(r RegistryClient) {

	Timeout := time.Duration(3 * time.Second)
	client := &http.Client{Timeout: Timeout}

	req, err := http.NewRequest(Get.Name, "http://"+r.Ip+":"+r.Port+"/v2", nil)
	if err != nil {
		panic(err.Error())
	}
	_, err = client.Do(req)
	if err != nil {
		log.Errorf("registry[%s]:healthCheck fail:%s", r.Ip+":"+r.Port, err.Error())
		r.illChannel <- 1
	}
}

func NewRegisryClient() *RegistryClient {
	ip := "192.168.15.119"
	port := "5000"
	var c2 = new(RegistryClient)

	c2.Ip = ip
	c2.Port = port
	opts := ClientOpts{
		Url:     "http://" + c2.Ip + ":" + c2.Port,
		Timeout: 3 * time.Second,
	}
	c2.Opts = &opts
	c2.illChannel = make(chan int)

	return c2

}
func deployRegisteredClient() {
}

func main() {
	log.Level = logrus.DebugLevel

	strHost, err := ioutil.ReadFile(HostJson)
	if err != nil {
		log.Fatalf("config remote host fail:", err)
	}
	log.Debug(string(strHost))

	var clients Clients
	err = json.Unmarshal(strHost, &clients)
	if err != nil {
		log.Fatalf("json unmarshal fail:", err)
	}

	for _, v := range clients.Hosts {
		log.Debugf("%s:%s\n", v.Ip, v.Port)
		mMap.Lock()
		clientMap[v.Ip+":"+v.Port] = &v
		mMap.Unlock()
		//创建ill channel
		v.illChannel = make(chan int)
	}

	var ilist imagelist
	strImage, err := ioutil.ReadFile(ImageJson)
	if err != nil {
		log.Fatalf("config remote host fail:", err)
	}
	log.Debug(string(strImage))

	err = json.Unmarshal(strImage, &ilist)
	if err != nil {
		log.Fatalf("json unmarshal fail:", err)
	}

	log.Debug(clientMap)
	return

	mMap.RLock()
	for _, client := range clientMap {
		client.DoAction("/list", Get)
	}
	mMap.RUnlock()

	//	registry healthy check
	log.Info("start registry client, healthCheck")
	registry := NewRegisryClient()
	go func(r RegistryClient) {
		for {
			log.Debugf("health check[%s]", r.Ip+":"+r.Port)
			registryHealthCheck(r)
			time.Sleep(3 * time.Second)
		}
	}(*registry)

	go func() {
		go func() {
			//标记已推送镜像
			for _, i := range ilist.images {
				url := i.Name + "/" + "manifests" + "/" + i.Tag
				resp, err := registry.DoAction(url, Get)
				if err == nil {
					if resp.StatusCode == http.StatusOK {
						i.state = pushed
						pMap.Lock()
						pushedMap[i.Name+":"+i.Tag] = 1
						pMap.Unlock()
					}
				}
			}
			//主机存在指定镜像,优先推送
			for _, i := range ilist.images {
				for _, c := range clientMap {
					url := "/search/" + i.Name
					c.DoAction(url, Get)
					// if i in list
					//set working go push
					// c.DoAction(push)
					//
				}
			}

			//从公有仓库拉取镜像,
			for _, c := range clientMap {
				if c.state == working {
					continue
				}

				c.state = working
				url := ""
				go func(c Client) {
					c.DoAction(url, Get)
					c.state = free
				}(*c)
			}

			for _, c := range clientMap {
				//url := "/search/" + image + "/" + tag
				url := ""
				c.DoAction(url, Get)
			}
		}()
	}()

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

	//添加新的测试机

	go func() {
		log.Info("start register server")
		mux := &dockerMux{}
		http.ListenAndServe(":12345", mux)
	}()

	log.Info("start to run test clients...")
	log.Debug(clientMap)
	mMap.RLock()
	for _, client := range clientMap {
		go func(c Client) {
			go healthCheck(c)
			for {
				select {
				case _ = <-c.illChannel:
					mMap.Lock()
					delete(clientMap, c.Ip+":"+c.Port)
					mMap.Unlock()
					runtime.Goexit()
				default:
					randTest(c)
				}
			}
		}(*client)
	}
	log.Info("started all registered client...")
	mMap.RUnlock()
	_ = <-registry.illChannel //阻塞主线程,registry健康监测失败才退出
	fmt.Println("registry healthy check fail, exit...")
}
