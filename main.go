package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
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
)

var (
	clientMap          = make(map[string]*Client) //map[ip]Client
	mMap               = new(sync.RWMutex)
	registerChannel    = make(chan Client)
	pushedMap          = make(map[string]int)
	pMap               = new(sync.RWMutex)
	actions            = []string{"pull", "push"}
	log                = logrus.New()
	HostJson           = "./host.json"
	ImageJson          = "./image.json"
	imageMap           = make(map[string]*Image)
	iMap               = new(sync.RWMutex)
	appSoarRegistry    = "192.168.15.119"
	appSoarPort        = "5000"
	RegisterListenPort = "12345"
	//repo只能支持小写
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
	//	User       string `json:"user"`
	//	Password   string `json:"password"`
}

type RegistryClient struct {
	Client
}

type Imagelist struct {
	Images []Image `json:"images"`
}

type Image struct {
	Name  string `json:"name"`
	Tag   string `json:"tag"`
	state int
}

func pushImageExists(c2 Client) {
	log.Debugf("%s:push image exists in local", c2.Ip)
	list, err := c2.getImages()
	if err != nil {
		log.Errorf("cant not get image list")
		return
	}
	//尝试推送当前已存在的镜像
	for _, j := range list {
		if i, exists := imageMap[j.Image]; exists {
			if i.state == unpull {
				log.Debugf("%s is unpulled", i.Name)
				iMap.Lock()
				//someOne is handlering this image
				if i.state != unpull {
					iMap.Unlock()
					continue
				} else {
					log.Debugf("%s:pushing local %s", c2.Ip, j.Image)
					i.state = pushing
					iMap.Unlock()
					err := c2.pushImage(j.Image)
					iMap.Lock()
					if err != nil {
						log.Error(err)
						i.state = unpull
					} else {
						i.state = pushed
					}
					iMap.Unlock()
					pMap.Lock()
					pushedMap[j.Image] = 1
					pMap.Unlock()
				}
			}
		}
	}
}

func pushImageNotExists(c2 Client) {
	log.Debugf("%s:push image not exists in local", c2.Ip)
	for _, i := range imageMap {
		if i.state == unpull {
			log.Debugf("%s:try to push %s", c2.Ip, i.Name)
			iMap.Lock()
			if i.state != unpull {
				iMap.Unlock()
				continue
			} else {
				i.state = pulling
				iMap.Unlock()
				err := c2.pullImageFromPublic(i.Name + ":" + i.Tag)
				log.Debugf("%s:pulling from remote %s", c2.Ip, i.Name+":"+i.Tag)
				if err == nil {
					log.Debugf("%s:pulling from  public success %s", c2.Ip, i.Name+":"+i.Tag)
					slice := strings.SplitN(i.Name, "/", 2)
					newrepo := appSoarRegistry + ":" + appSoarPort + "/" + slice[1]

					log.Debugf("new repo %v:%v", newrepo, i.Tag)
					err = c2.tagImage(i.Name+":"+i.Tag, newrepo+":"+i.Tag)
					if err != nil {
						log.Errorf("%s:tag image[%v ==> %v] fail:%v", c2.Ip, i.Name+":"+i.Tag, newrepo+":"+i.Tag, err)
						iMap.Lock()
						i.state = unpull
						iMap.Unlock()
						return
					}

					err = c2.pushImage(newrepo + ":" + i.Tag)
					if err != nil {
						log.Errorf("%s:push fail:%v", c2.Ip, err)
						iMap.Lock()
						i.state = unpull
						iMap.Unlock()
					} else {
						log.Debugf("%s:pushing remote to registry %s", c2.Ip, i.Name+":"+i.Tag)
						iMap.Lock()
						i.state = pulled
						iMap.Unlock()
						pMap.Lock()
						pushedMap[newrepo+":"+i.Tag] = 1
						pMap.Unlock()
					}

				} else {
					log.Errorf("%s:pulling from public fail:%v", c2.Ip, err)
					iMap.Lock()
					i.state = unpull

					log.Error(err)
				}
			}
		} else {
			log.Debugf("%s: %s is pulling or pulled or pushed", c2.Ip, i.Name)
		}
	}
}

func register(w http.ResponseWriter, r *http.Request) {
	log.Debug("start to register ..............." + r.RemoteAddr)
	vars := mux.Vars(r)
	port := vars["port"]
	log.Debug(r.RemoteAddr)
	log.Debug(port)
	slice := strings.Split(r.RemoteAddr, ":")

	c2 := new(Client)
	c2.Ip = slice[0]
	c2.Port = port
	c2.Opts = new(ClientOpts)
	c2.Opts.Url = "http://" + c2.Ip + ":" + c2.Port
	c2.Opts.Timeout = 1000 * time.Second
	c2.illChannel = make(chan int, 10)
	c2.state = 0

	//注册新的主机,或者修改旧的主机
	mMap.Lock()
	clientMap[c2.Ip+":"+c2.Port] = c2
	mMap.Unlock()
	//新注册的client开始先检测有没有镜像是处于unpull
	//最好起个goroutine
	//否则客户端一直得不到回应
	go func() {
		log.Debug("client prepare registry's environment")
		pushImageExists(*c2)
		//可能还有未推送的,且不在本地的镜像
		pushImageNotExists(*c2)

		go func(c Client) {
			go healthCheck(c)
			//开启10个goroutine进行随机测试
			for i := 0; i < 10; i++ {
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
						time.Sleep(2 * time.Second)
					}
				}(c)
			}

		}(*c2)
	}()
	return
}

type ClientImageList struct {
	Image string `json:"image"`
}

func (c Client) getImages() ([]ClientImageList, error) {
	var apiImages []ClientImageList
	resp, err := c.DoAction("/list", Get)
	if err != nil {
		log.Errorf("remote get image fail:%v", err)
		return []ClientImageList{}, err
	}
	defer func() {
		if resp != nil {
			resp.Body.Close()
		}
	}()
	byteContent, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Errorf("io read fail :%v", err)
		return []ClientImageList{}, err
	}
	err = json.Unmarshal(byteContent, &apiImages)
	if err != nil {
		return []ClientImageList{}, errors.New("json unmarshal apiimage fail")
	}

	return []ClientImageList{}, nil
}

func (c Client) pullImage(imgTag string) error {
	slice1 := strings.Split(imgTag, ":")

	var img string
	img = slice1[0]
	for i := 1; i < len(slice1)-1; i++ {
		img = img + ":" + slice1[i]
	}
	tag := slice1[len(slice1)-1]

	log.Debugf("%s:pullImage:%v,tag:%v", c.Ip, img, tag)

	resp, err := c.DoAction("/pull/"+img+"/"+tag, Get)
	if err != nil {
		log.Errorf("%s:pull Image[%s:%s] fail:%v", c.Ip, img, tag, err)
		return err
	}
	defer func() {
		if resp != nil {
			resp.Body.Close()
		}
	}()

	if resp.StatusCode != http.StatusOK {
		log.Errorf("%s:pull image[%s:%s] fail", c.Ip, img, tag)
		return errors.New("pull image fail:" + resp.Status)
	}

	return nil

}

func (c Client) pushImage(imgTag string) error {
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
		pushedMap[img+":"+tag] = 1
	} else {
		log.Errorf("%s:push image[%s:%s] fail", c.Ip, img, tag)
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
		log.Errorf("%s:tag image[%s ==> %s] fail", c.Ip, imgTag, new)
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
		fmt.Println(err)
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
			fmt.Println(err)
			return err
		} else {
			fmt.Println(resp.StatusCode)
			return errors.New(string(byteContent))
		}
	}
}

func randTest(client Client) error {
	//worker is free
	log.Debugf("%s: start rand test....", client.Ip)
	if client.state == 0 {

		lPushedMap := len(pushedMap)
		if lPushedMap == 0 {
			log.Debugf("randtest without pushed image")
			return errors.New("test without pushed image")
		}

		rand.Seed(time.Now().Unix())

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
			//tag repository
			slice1 := strings.Split(image, ":")
			tag := slice1[len(slice1)-1]

			newrepo := appSoarRegistry + ":" + appSoarPort + "/" + RandStringRunes(6) + "/" + RandStringRunes(12)
			log.Debugf("%s: is push new image:%s:%s", client.Ip, newrepo, tag)
			err := client.tagImage(image, newrepo+":"+tag)
			if err != nil {
				log.Debugf("%s: tag new image:%s:%s fail:%v", client.Ip, newrepo, tag, err)
				return err
			}

			err = client.pushImage(newrepo + ":" + tag)
			if err != nil {
				log.Debugf("%s: push new image:%s:%s fail:%v", client.Ip, newrepo, tag, err)
				return err
			} else {
				log.Debugf("%s: push new image:%s:%s success", client.Ip, newrepo, tag)

			}
			pMap.Lock()
			pushedMap[newrepo+":"+tag] = 1
			pMap.Unlock()
		case "pull":
			log.Debugf("%s: is pulling  image:%s", client.Ip, image)
			err := client.pullImage(image)
			if err != nil {
				log.Debugf("%s: is pulling  image:%s fail: %v", client.Ip, image, err)
			} else {
				log.Debugf("%s: is pulling  image:%s success", client.Ip, image)
			}

		default:
			log.Errorf("invalid action")
		}
	}
	return nil
}

func healthCheck(c Client) {
	Timeout := time.Duration(3 * time.Second)
	client := &http.Client{Timeout: Timeout}

	req, err := http.NewRequest(Get.Name, "http://"+c.Ip+":"+c.Port+"/ping", nil)
	if err != nil {
		log.Error(err)
		for i := 0; i < 10; i++ {
			c.illChannel <- 1
		}
	}

	_, err = client.Do(req)
	if err != nil {
		for i := 0; i < 10; i++ {
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
	rand.Seed(time.Now().Unix())
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]

	}
	return string(b)
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

	var ilist Imagelist
	strImage, err := ioutil.ReadFile(ImageJson)
	if err != nil {
		log.Fatalf("config remote host fail:", err)
	}

	log.Debug(string(strImage))

	err = json.Unmarshal([]byte(strImage), &ilist)
	if err != nil {
		log.Fatalf("json unmarshal fail:", err)
	}
	log.Debug(ilist)

	if len(ilist.Images) == 0 {
		log.Fatalf("imageJson doesn't set any image data")
	}

	for _, i := range ilist.Images {
		imageMap[i.Name+":"+i.Tag] = &i
	}

	//	registry healthy check
	log.Info("start registry client, healthCheck")
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
		log.Info("start register server")
		err := http.ListenAndServe(":"+RegisterListenPort, router)
		if err != nil {
			panic(err)
		}
	}()

	_ = <-registry.illChannel //阻塞主线程,registry健康监测失败才退出
	fmt.Println("registry healthy check fail, exit...")
}
