package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"math/rand"

	"github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
)

const (
	pushed  = 4
	pushing = 3
	pulled  = 2
	pulling = 1
	unpull  = 0

	working = 1
	free    = 0

	//默认产生的随机测试goroutine数
	defaultRandNum = 10
)

var (
	clientMap          = make(map[string]*Client) //map[ip]Client,移除?
	mMap               = new(sync.RWMutex)        //用于同步clientMap访问
	pushedMap          = make(map[string]int)
	pMap               = new(sync.RWMutex)        //用于同步pushedMap访问
	actions            = []string{"pull", "push"} //执行的动作
	log                = logrus.New()
	HostJson           = "./host.json"
	ImageJson          = "./image.json"
	imageMap           = make(map[string]Image)
	iMap               = new(sync.RWMutex)
	appSoarRegistry    string
	appSoarPort        string
	RegisterListenPort string
	//repo名只支持小写, 用于随机产生repo名
	letterRunes = []rune("abcdefghijklmnopqrstuvwxyz")
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
}

//访问registry server的客户端
type RegistryClient struct {
	Client
}

//镜像列表
type Imagelist struct {
	Images []Image `json:"images"`
}

type Image struct {
	Name  string `json:"name"`
	Tag   string `json:"tag"`
	state int
}

//推送存在于本地的镜像
func pushImageExists(c2 Client) {
	log.Debugf("%s:push image exists in local", c2.Ip)
	list, err := c2.getImages()
	if err != nil {
		log.Errorf("%s:can not get image list:%v", c2.Ip, err)
		return
	}
	log.Debugf("%s image list:%v\n", c2.Ip, list)
	//尝试推送当前已存在的镜像
	for _, j := range list {
		if i, exists := imageMap[j.ImageTag]; exists {
			if i.state == unpull {
				log.Debugf("%s is unpulled", i.Name)
				iMap.Lock()
				//someOne is handlering this image
				if i.state != unpull {
					iMap.Unlock()
					continue
				} else {
					log.Debugf("%s:pushing local %s", c2.Ip, j.ImageTag)
					i.state = pushing
					imageMap[j.ImageTag] = i
					iMap.Unlock()

					newrepo := appSoarRegistry + ":" + appSoarPort
					imagePush := strings.Replace(j.ImageTag, "docker.io", newrepo, 1)
					log.Debugf("%s:tag [%v==>%v] when pushing", c2.Ip, j.ImageTag, imagePush)
					err := c2.tagImage(j.ImageTag, imagePush)
					if err != nil {
						log.Errorf("%s:tag image[%v ==> %v] fail:%v", c2.Ip, j.ImageTag, imagePush, err)
						iMap.Lock()
						i.state = unpull
						imageMap[j.ImageTag] = i
						iMap.Unlock()
						continue
					}

					err = c2.pushImage(imagePush)
					if err != nil {
						log.Errorf("%s:fail to push local image to %s for %v:", c2.Ip, appSoarRegistry+":"+appSoarPort, err)
						iMap.Lock()
						i.state = unpull
						imageMap[j.ImageTag] = i
						iMap.Unlock()
					} else {
						iMap.Lock()
						i.state = pushed
						imageMap[j.ImageTag] = i
						iMap.Unlock()
						pMap.Lock()
						pushedMap[imagePush] = 1
						imageMap[j.ImageTag] = i
						pMap.Unlock()
					}
				}
			}
		}
	}
}

func pushImageNotExists(c2 Client) {
	log.Debugf("%s:push image not exists in local", c2.Ip)
	for k, i := range imageMap {
		if i.state == unpull {
			log.Debugf("%s:try to push %s ", c2.Ip, i.Name)
			iMap.Lock()
			if i.state != unpull {
				iMap.Unlock()
				continue
			} else {
				i.state = pulling
				imageMap[k] = i
				iMap.Unlock()

				tmpImageTag := i.Name + ":" + i.Tag
				err := c2.pullImageFromPublic(tmpImageTag)
				log.Debugf("%s:pulling from remote %s", c2.Ip, tmpImageTag)
				if err == nil {
					log.Debugf("%s:pulling from  public success %s", c2.Ip, tmpImageTag)
					imagePush := strings.Replace(tmpImageTag, "docker.io", appSoarRegistry+":"+appSoarPort, 1)

					log.Debugf("image push to registry :[%s]", imagePush)
					err = c2.tagImage(tmpImageTag, imagePush)
					if err != nil {
						log.Errorf("%s:tag image[%v ==> %v] fail:%v", c2.Ip, tmpImageTag, imagePush, err)
						iMap.Lock()
						i.state = unpull
						imageMap[k] = i
						iMap.Unlock()
						continue
					}
					log.Debugf("%s:tag image[%v ==> %v] ok", c2.Ip, tmpImageTag, imagePush)

					iMap.Lock()
					if i.state == pushing || i.state == pushed {
						iMap.Unlock()
						continue
					} else {
						i.state = pushing
						imageMap[k] = i
						iMap.Unlock()
					}

					err = c2.pushImage(imagePush)
					if err != nil {
						log.Errorf("%s:push fail:%v", c2.Ip, err)
						iMap.Lock()
						i.state = unpull
						imageMap[k] = i
						iMap.Unlock()
					} else {
						log.Debugf("%s:push remote to registry %s success", c2.Ip, i.Name+":"+i.Tag)
						iMap.Lock()
						log.Debugf("%s: set %s's state to 4:%p", c2.Ip, i.Name+":"+i.Tag, &i)
						i.state = pushed
						imageMap[k] = i
						iMap.Unlock()
						pMap.Lock()
						pushedMap[imagePush] = 1
						pMap.Unlock()
					}

				} else {
					log.Errorf("%s:pulling from public fail:%v", c2.Ip, err)
					iMap.Lock()
					if i.state != pushing || i.state != pushed {
						i.state = unpull
						imageMap[k] = i
					}
					iMap.Unlock()
					log.Error(err)
				}
			}
		} else {
			log.Infof("%s: %s is pulling or pulled or pushed", c2.Ip, i.Name)
		}
	}
}

func register(w http.ResponseWriter, r *http.Request) {
	log.Infof("%s:start to register ", r.RemoteAddr)
	vars := mux.Vars(r)
	port := vars["port"]
	log.Debug(r.RemoteAddr)
	log.Debug(port)
	slice := strings.Split(r.RemoteAddr, ":")

	//创建新的client object
	var c2 Client
	c2.Ip = slice[0] //注册主机的IP
	c2.Port = port   //注册主机通信端口
	c2.Opts = new(ClientOpts)
	c2.Opts.Url = "http://" + c2.Ip + ":" + c2.Port
	c2.Opts.Timeout = 1000 * time.Second           //限制连接时长
	c2.illChannel = make(chan int, defaultRandNum) //当主机health check失败后, 将通知启动的协程不再进行随机测试，自行销毁
	c2.state = 0                                   //移除?

	//注册新的主机,或者修改旧的主机
	mMap.Lock()
	clientMap[c2.Ip+":"+c2.Port] = &c2
	mMap.Unlock()
	//新注册的client开始先检测有没有镜像是处于unpull
	//最好起个goroutine
	//否则客户端一直得不到回应
	go func() {
		log.Debug("client prepare registry's environment")
		go func() {
			for {
				healthCheck(c2)
				time.Sleep(1 * time.Second)
			}
		}()
		for i, j := range imageMap {
			log.Debugf("prepare: %v === %v\n", i, j)
		}
		for {
			allPushed := true
			pushImageExists(c2)
			//可能还有未推送的,且不在本地的镜像
			pushImageNotExists(c2)
			for _, j := range imageMap {
				log.Debugf("prepare env ========>image:%s,state:%d:j:%p\n", j.Name, j.state, &j)
				if j.state != pushed {
					allPushed = false
				}
			}

			if allPushed {
				break
			}
			time.Sleep(2 * time.Second)
		}

		//开启10个goroutine进行随机测试
		for i := 0; i < defaultRandNum; i++ {
			go func(c Client) {
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
					//for test
				}
			}(c2)
		}
	}()

	msg := appSoarRegistry + ":" + appSoarPort
	fmt.Fprintf(w, msg)
	return
}

//解析客户端镜像列表
type ClientImageList struct {
	ImageTag string `json:"image"`
}

func (c Client) getImages() ([]ClientImageList, error) {
	var apiImages []ClientImageList
	resp, err := c.DoAction("/list", Get)
	if err != nil {
		log.Debugf("%s: get image fail:%v\n", c.Ip, err)
		return []ClientImageList{}, err
	}
	defer func() {
		if resp != nil {
			resp.Body.Close()
		}
	}()
	byteContent, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Debugf("%s: get image fail for %v\n", c.Ip, err)
		return []ClientImageList{}, err
	}
	err = json.Unmarshal(byteContent, &apiImages)
	if err != nil {
		log.Debugf("%s: get image fail for %v\n", c.Ip, err)
		return []ClientImageList{}, errors.New("json unmarshal apiimage fail")
	}

	return apiImages, nil
}

func (c Client) pullImage(imgTag string) error {
	slice1 := strings.Split(imgTag, ":")

	var img string
	img = slice1[0]
	for i := 1; i < len(slice1)-1; i++ {
		img = img + ":" + slice1[i]
	}
	tag := slice1[len(slice1)-1]

	log.Debugf("%s:pullImage:%v,tag:%v\n", c.Ip, img, tag)

	resp, err := c.DoAction("/pull/"+img+"/"+tag, Get)
	if err != nil {
		log.Debugf("%s:pull Image[%s:%s] fail:%v", c.Ip, img, tag, err)
		return err
	}
	defer func() {
		if resp != nil {
			resp.Body.Close()
		}
	}()

	if resp.StatusCode != http.StatusOK {
		log.Debugf("%s:pull image[%s:%s] fail\n", c.Ip, img, tag)
		return errors.New("pull image fail:" + resp.Status)
	}

	return nil

}

func (c Client) pushImage(imgTag string) error {
	log.Debugf("%s pushImage try to push [%s]", c.Ip, imgTag)
	slice1 := strings.Split(imgTag, ":")

	var img string
	img = slice1[0]
	for i := 1; i < len(slice1)-1; i++ {
		img = img + ":" + slice1[i]
	}
	tag := slice1[len(slice1)-1]

	log.Debugf("%s:pushImage image:%v,tag:%v", c.Ip, img, tag)

	url := "/push/" + img + "/" + tag
	log.Debugf("%s: url:%s", c.Ip, url)
	resp, err := c.DoAction(url, Get)
	if err != nil {
		log.Debugf("%s: fail to push image[%s] for %v\n", c.Ip, imgTag, err)
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
		pushedMap[img+":"+tag] = 1
	} else {
		log.Debugf("%s:push image[%s:%s] fail\n", c.Ip, img, tag)
		return errors.New("push image fail:" + resp.Status)
		//record error
	}
	return nil
}

type TagOption struct {
	New string `json:"new"`
	Old string `json:"old"`
}

func (c Client) tagImage(imgTag string, new string) error {

	opts := TagOption{New: new, Old: imgTag}

	byteData, err := json.Marshal(opts)
	if err != nil {
		return err
	}
	url := "/tag"
	resp, err := c.DoPost(url, byteData)
	if err != nil {
		log.Debugf("%s: fail to tag image[%s ==> %s] for %v", c.Ip, imgTag, new, err)
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
	} else {
		//record error
		log.Debugf("%s:fail to tag image[%s ==> %s] for %v ", c.Ip, imgTag, new, resp.Status)
		return errors.New("tag fail:" + resp.Status)
	}
	return nil
}

func hello(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "hello")
}

func (c Client) pullImageFromPublic(imgTag string) error {
	slice1 := strings.Split(imgTag, ":")

	var img string
	img = slice1[0]
	for i := 1; i < len(slice1)-1; i++ {
		img = img + ":" + slice1[i]
	}
	tag := slice1[len(slice1)-1]

	log.Debugf("%s:pushImageFromPublic image:%v,tag:%v", c.Ip, img, tag)

	url := "/download/" + img + "/" + tag
	log.Debugf("%s:url:%s", c.Ip, url)
	resp, err := c.DoAction(url, Get)
	if err != nil {
		log.Debugf("%s: fail to pull image[%s] from public for %v\n", c.Ip, imgTag, err)
		return err
	}
	defer func() {
		if resp != nil {
			resp.Body.Close()
		}
	}()

	if resp.StatusCode == http.StatusOK {
		return nil
	} else {
		byteContent, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Debug(err)
			return err
		} else {
			log.Debugf("%s: fail to pull image[%s] from public for %v\n", c.Ip, imgTag, resp.Status)
			return errors.New(string(byteContent))
		}
	}
}

func randTest(client Client) error {
	log.Debugf("%s: start rand test....", client.Ip)
	if client.state == 0 {
		lPushedMap := len(pushedMap)
		if lPushedMap == 0 {
			log.Debugf("randtest without pushed image")
			return errors.New("test without pushed image")
		}

		iList := make([]string, lPushedMap)
		i := 0
		for j, _ := range pushedMap {
			iList[i] = j
			i++
		}
		image := iList[rand.Intn(len(iList))]

		k := rand.Intn(len(actions))
		switch actions[k] {
		case "push":
			//tag repository
			slice1 := strings.Split(image, ":")
			tag := slice1[len(slice1)-1]

			newrepo := appSoarRegistry + ":" + appSoarPort + "/" + RandStringRunes(6) + "/" + RandStringRunes(7)
			log.Infof("%s: is pushint new image[%s:%s]\n", client.Ip, newrepo, tag)
			err := client.tagImage(image, newrepo+":"+tag)
			if err != nil {
				log.Errorf("%s: push new image fail for tag new image[%s ==> %s:%s] fail:%v\n", client.Ip, image, newrepo, tag, err)
				return err
			}

			err = client.pushImage(newrepo + ":" + tag)
			if err != nil {
				log.Errorf("%s: push new image[%s:%s] fail:%v\n", client.Ip, newrepo, tag, err)
				return err
			} else {
				log.Infof("%s: push new image[%s:%s] success\n", client.Ip, newrepo, tag)

			}
			pMap.Lock()
			pushedMap[newrepo+":"+tag] = 1
			pMap.Unlock()
		case "pull":
			log.Infof("%s: is pulling image:%s", client.Ip, image)
			err := client.pullImage(image)
			if err != nil {
				log.Errorf("%s: pull image[%s] fail: %v\n", client.Ip, image, err)
			} else {
				log.Infof("%s: pull image[%s] success\n", client.Ip, image)
			}

		default:
			log.Errorf("%s:invalid action", client.Ip)
		}
	}
	return nil
}

func healthCheck(c Client) {
	Timeout := time.Duration(3 * time.Second)
	client := &http.Client{Timeout: Timeout}

	req, err := http.NewRequest(Get.Name, "http://"+c.Ip+":"+c.Port+"/ping", nil)
	if err != nil {
		log.Errorf("%s health check fail for %v", c.Ip, err)
		//让产生的10个goroutine自杀
		for i := 0; i < defaultRandNum; i++ {
			c.illChannel <- 1
		}
	}

	_, err = client.Do(req)
	if err != nil {
		for i := 0; i < defaultRandNum; i++ {
			c.illChannel <- 1
		}
	}
}

func registryHealthCheck(r RegistryClient) {

	Timeout := time.Duration(3 * time.Second)
	client := &http.Client{Timeout: Timeout}

	req, err := http.NewRequest(Get.Name, "http://"+r.Ip+":"+r.Port+"/v2", nil)
	if err != nil {
		log.Error(err)
		r.illChannel <- 1
	}
	_, err = client.Do(req)
	if err != nil {
		log.Errorf("registry[%s]:healthCheck fail:%s", r.Ip+":"+r.Port, err.Error())
		r.illChannel <- 1
	}
}

func NewRegisryClient() *RegistryClient {
	var c2 = new(RegistryClient)

	c2.Ip = appSoarRegistry
	c2.Port = appSoarPort
	opts := ClientOpts{
		Url:     "http://" + c2.Ip + ":" + c2.Port,
		Timeout: 3 * time.Second,
	}
	c2.Opts = &opts
	c2.illChannel = make(chan int)

	return c2

}

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func init() {

	log.Level = logrus.DebugLevel
	rand.Seed(time.Now().Unix())

	//flag.StringVar(&appSoarRegistry, "rip", "192.168.15.119", "registry ip")
	//flag.StringVar(&appSoarPort, "rport", "5000", "registry port")
	//flag.StringVar(&RegisterListenPort, "lport", "12345", "register server listen port")
	flag.StringVar(&appSoarRegistry, "rip", "", "registry ip")
	flag.StringVar(&appSoarPort, "rport", "", "registry port")
	flag.StringVar(&RegisterListenPort, "lport", "", "register server listen port")
	var logFile string
	flag.StringVar(&logFile, "log", "./autotest.log", "log file path")
	flag.Parse()
	if len(appSoarRegistry) == 0 || len(appSoarPort) == 0 || len(RegisterListenPort) == 0 {
		panic("invalid argument ")
	}

	log.Formatter = &logrus.TextFormatter{DisableColors: true}
	fp, err := os.OpenFile(logFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644) // 让程序关闭后,自动释放
	if err != nil {
		log.Errorf("Create logfile %s fail\n", logFile)
		os.Exit(1)
	}
	log.Out = fp

	var ilist Imagelist
	strImage, err := ioutil.ReadFile(ImageJson)
	if err != nil {
		log.Fatalf("server start fail:", err)
	}

	log.Debug(string(strImage))

	err = json.Unmarshal([]byte(strImage), &ilist)
	if err != nil {
		log.Fatalf("server start fail: Image  unmarshal fail:", err)
	}
	log.Debug(ilist)

	if len(ilist.Images) == 0 {
		log.Fatalf("Server start fail: imageJson doesn't set any image data")
	}

	for _, i := range ilist.Images {
		log.Debugf("ilist :%s", i)
		if !strings.HasPrefix(i.Name, "docker.io/") {
			i.Name = "docker.io/" + i.Name
		}

		imageMap[i.Name+":"+i.Tag] = i
	}
	for i, j := range imageMap {
		log.Infof("%v:%v\n", i, j)
	}

}

func main() {

	//	registry healthy check
	log.Debugf("start registry client, healthCheck\n")
	registry := NewRegisryClient()
	go func(r RegistryClient) {
		for {
			registryHealthCheck(r)
			time.Sleep(3 * time.Second)
		}
	}(*registry)

	go func() {
		router := mux.NewRouter()
		router.Methods("GET").Path("/register/{port:[0-9]*}").HandlerFunc(register)
		router.Methods("GET").Path("/").HandlerFunc(hello)
		log.Info("start register server, listen on " + RegisterListenPort)
		err := http.ListenAndServe(":"+RegisterListenPort, router)
		if err != nil {
			log.Errorf("register server fail:%v", err)
			panic(err)
		}
	}()

	//阻塞主线程,registry健康监测失败才退出
	_ = <-registry.illChannel
	log.Info("registry healthy check fail, exit...")
}
