package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"logic/client"
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

	//默认产生的随机测试goroutine数
	defaultRandNum = 5
)

var (
	clientMap          = make(map[string]*client.Client) //map[ip]Client,移除?
	mMap               = new(sync.RWMutex)               //用于同步clientMap访问
	pushedMap          = make(map[string]int)
	pMap               = new(sync.RWMutex)        //用于同步pushedMap访问
	actions            = []string{"pull", "push"} //执行的动作
	log                = logrus.New()
	pushLog            = logrus.New()
	pullLog            = logrus.New()
	ImageJson          = "./image.json"
	imageMap           = make(map[string]Image)
	iMap               = new(sync.RWMutex)
	appSoarRegistry    string
	appSoarPort        string
	RegisterListenPort string
	//repo名只支持小写, 用于随机产生repo名
	pullLimit          int
	pushLimit          int
	countPullChan      = make(chan int) //负责统计pushed image
	countPushChan      = make(chan int) //负责统计pushed image
	limitChan          = make(chan int) //传递信号给limit killer
	LoginUser          string
	LoginPassword      string
	LoginServer        string
	Debug              bool
	workingPushChan    = make(chan int)
	workingPullChan    = make(chan int)
	stopRandTestSignal int
	working            int
	workingDone        = make(chan int)
)

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

func pushImageExists(c2 client.Client) {
	log.Debugf("%s:push image exists in local", c2.Ip)
	list, err := c2.GetImages()
	if err != nil {
		log.Errorf("%s:can not get image list:%v", c2.Ip, err)
		return
	}
	//	log.Debugf("%s image list:%v\n", c2.Ip, list)
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
					var imagePush string
					if strings.HasPrefix(j.ImageTag, "docker.io") {
						imagePush = strings.Replace(j.ImageTag, "docker.io", newrepo, 1)
					} else {
						imagePush = newrepo + "/" + j.ImageTag
					}
					log.Debugf("%s:tag [%v==>%v] when pushing", c2.Ip, j.ImageTag, imagePush)
					err := c2.TagImage(j.ImageTag, imagePush)
					if err != nil {
						log.Errorf("%s:tag image[%v ==> %v] fail:%v", c2.Ip, j.ImageTag, imagePush, err)
						iMap.Lock()
						i.state = unpull
						imageMap[j.ImageTag] = i
						iMap.Unlock()
						continue
					}

					err = c2.PushImage(imagePush)
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
						pMap.Unlock()
					}
				}
			}
		} else { //docker hub上拉取的镜像可能增加了docker.io前缀
			var tmp string
			if strings.HasPrefix(j.ImageTag, "docker.io/") {
				tmp = strings.TrimPrefix(j.ImageTag, "docker.io/")
				if i, exists := imageMap[tmp]; exists {
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
							imageMap[tmp] = i
							iMap.Unlock()

							newrepo := appSoarRegistry + ":" + appSoarPort
							var imagePush string
							imagePush = strings.Replace(j.ImageTag, "docker.io", newrepo, 1)
							log.Debugf("%s:tag [%v==>%v] when pushing", c2.Ip, j.ImageTag, imagePush)
							err := c2.TagImage(j.ImageTag, imagePush)
							if err != nil {
								log.Errorf("%s:tag image[%v ==> %v] fail:%v", c2.Ip, j.ImageTag, imagePush, err)
								iMap.Lock()
								i.state = unpull
								imageMap[tmp] = i
								iMap.Unlock()
								continue
							}

							err = c2.PushImage(imagePush)
							if err != nil {
								log.Errorf("%s:fail to push local image to %s for %v:", c2.Ip, appSoarRegistry+":"+appSoarPort, err)
								iMap.Lock()
								i.state = unpull
								imageMap[tmp] = i
								iMap.Unlock()
							} else {
								iMap.Lock()
								i.state = pushed
								imageMap[tmp] = i
								iMap.Unlock()
								pMap.Lock()
								pushedMap[imagePush] = 1
								pMap.Unlock()
							}
						}
					}
				}
			}
		}
	}
}

func pushImageNotExists(c2 client.Client) {
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
				if !strings.HasPrefix(tmpImageTag, "docker.io") {
					tmpImageTag = "docker.io/" + tmpImageTag
				}

				err := c2.PullImageFromPublic(tmpImageTag)
				log.Debugf("%s:pulling from remote %s", c2.Ip, tmpImageTag)
				var imagePush string
				if err == nil {
					log.Debugf("%s:pulling from  public success %s", c2.Ip, tmpImageTag)
					if strings.HasPrefix(tmpImageTag, "docker.io") {
						imagePush = strings.Replace(tmpImageTag, "docker.io", appSoarRegistry+":"+appSoarPort, 1)
					} else {
						imagePush = appSoarRegistry + ":" + appSoarPort + "/" + tmpImageTag
					}

					log.Debugf("image push to registry :[%s]", imagePush)
					err = c2.TagImage(tmpImageTag, imagePush)
					if err != nil {
						log.Errorf("%s:tag image[%v ==> %v] fail:%v", c2.Ip, tmpImageTag, imagePush, err)
						iMap.Lock()
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

					err = c2.PushImage(imagePush)
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

// 注册控制器
func register(w http.ResponseWriter, r *http.Request) {
	log.Infof("%s:start to register ", r.RemoteAddr)
	vars := mux.Vars(r)
	port := vars["port"]
	log.Debug(r.RemoteAddr)
	log.Debug(port)
	slice := strings.Split(r.RemoteAddr, ":")

	//创建新的client object
	var c2 client.Client
	c2.Ip = slice[0] //注册主机的IP
	c2.Port = port   //注册主机通信端口
	c2.Opts = new(client.ClientOpts)
	c2.Opts.Url = "http://" + c2.Ip + ":" + c2.Port
	c2.Opts.Timeout = 1000 * time.Second           //限制连接时长
	c2.IllChannel = make(chan int, defaultRandNum) //当主机health check失败后, 将通知启动的协程不再进行随机测试，自行销毁
	c2.User = LoginUser
	c2.Password = LoginPassword
	c2.Server = LoginServer

	log.Infof("%s:%s login, password:%s, server:%s\n", c2.Ip, c2.User, c2.Password, c2.Server)
	err := c2.Login()
	if err != nil {
		log.Errorf("%s:%s login fail for %v\n", c2.Ip, c2.User, err)
		return
	}
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
				client.HealthCheck(c2, defaultRandNum)
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
		//开启指定数量goroutine进行随机测试
		for i := 0; i < defaultRandNum; i++ {
			go func(c client.Client, i int) {
				for {
					if stopRandTestSignal == 1 {
						runtime.Goexit()
					}
					select {
					case _ = <-c.IllChannel:
						mMap.Lock()
						delete(clientMap, c.Ip+":"+c.Port)
						mMap.Unlock()
						runtime.Goexit()
						//达到Limit,退出
					default:
						randTest(c)
					}
				}
			}(c2, i)
		}
	}()

	//
	msg := appSoarRegistry + ":" + appSoarPort
	fmt.Fprintf(w, msg)
	return
}

//健康检查器
func hello(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "hello")
}

func randTest(client client.Client) error {

	log.Debugf("%s: start rand test....", client.Ip)
	pMap.Lock()
	lPushedMap := len(pushedMap)
	if lPushedMap == 0 {
		log.Debugf("randtest without pushed image")
		pMap.Unlock()
		return errors.New("test without pushed image")
	}

	iList := make([]string, 0)
	for j, _ := range pushedMap {
		iList = append(iList, j)
	}
	pMap.Unlock()

	image := iList[rand.Intn(len(iList))]
	if len(image) == 0 {
		return errors.New("skip empty image string")
	}

	k := rand.Intn(len(actions))
	switch actions[k] {
	case "push":
		countPushChan <- 0
		_ = <-workingPushChan
		exists, err := client.CheckImageExists(image)
		if err != nil {
			countPushChan <- 1
			log.Infof("%s:pushImage fail for check image[%s] exists fail for %v", client.Ip, image, err)
			return err
		}

		if !exists {
			err := client.PullImage(image)
			if err != nil {
				countPushChan <- 1
				log.Infof("%s:pushImage fail for pull nonexist image[%s] for %v", client.Ip, image, err)
				return err
			}
		}

		slice1 := strings.Split(image, ":")
		tag := slice1[len(slice1)-1]

		newrepo := appSoarRegistry + ":" + appSoarPort + "/" + RandStringRunes(6) + "/" + RandStringRunes(7)
		log.Infof("%s: is pushing new image[%s:%s]\n", client.Ip, newrepo, tag)
		err = client.TagImage(image, newrepo+":"+tag)
		if err != nil {
			countPushChan <- 1
			log.Errorf("%s: push new image fail for tag new image[%s ==> %s:%s] fail:%v\n", client.Ip, image, newrepo, tag, err)
			return err
		}

		err = client.PushImage(newrepo + ":" + tag)
		if err != nil {
			countPushChan <- 1
			log.Errorf("%s: push new image[%s:%s] fail:%v\n", client.Ip, newrepo, tag, err)
			return err
		} else {
			pMap.Lock()
			pushedMap[newrepo+":"+tag] = 1
			pMap.Unlock()

			log.Infof("%s: push new image[%s:%s] success\n", client.Ip, newrepo, tag)
		}
		countPushChan <- 2
		pMap.Lock()
		pushedMap[newrepo+":"+tag] = 1
		pMap.Unlock()
		//workingChan <- -1
	case "pull":
		//workingChan <- 1
		countPullChan <- 0
		_ = <-workingPullChan
		log.Infof("%s: is pulling image:%s", client.Ip, image)
		err := client.PullImage(image)
		if err != nil {
			countPullChan <- 1
			log.Errorf("%s: pull image[%s] fail: %v\n", client.Ip, image, err)
		} else {
			countPullChan <- 2
			log.Infof("%s: pull image[%s] success\n", client.Ip, image)
		}

	default:
		log.Errorf("%s:invalid action", client.Ip)
	}
	return nil
}

func pushImageCount() {
	pushImageCount := 0
	pushFailCount := 0
	pushSuccessCount := 0
	c := new(sync.Mutex)
	//pullFailCount := 0
	//pullSuccessCount := 0
	for {
		i := <-countPushChan // 0 - 统计, 1 - 失败统计, 2 - 成功统计
		switch i {
		case 0:
			c.Lock()
			pushImageCount += 1
			log.Infof("pushCount:%d\n", pushImageCount)
			if pushImageCount > pushLimit {
				if stopRandTestSignal == 0 {
					stopRandTestSignal = 1
					go checkWorking(1)
				}
				c.Unlock()
				continue
				//limitChan <- 1
				//因为没有workingPushChan传递信号,routine都会阻塞,
				//不会继续执行
			}
			c.Unlock()
			working += 1
			workingPushChan <- 1
		case 1:
			pushFailCount += 1
			pushLog.Infof("push[success]:%d,[fail]:%d", pushSuccessCount, pushFailCount)
			working -= 1
		//	recordLog.Infof("push[fail]:%d", pushFailCount)
		case 2:
			pushSuccessCount += 1
			pushLog.Infof("push[success]:%d,[fail]:%d", pushSuccessCount, pushFailCount)
			working -= 1
		default:
			log.Errorf("pushImageCount invalid couthNum %d", i)
		}
	}
}

//监测正在工作的测试
func checkWorking(i int) {
	for {
		//所有工作已完成,发送终止信号
		if working == 0 {
			limitChan <- i
		}
		time.Sleep(50 * time.Microsecond)
	}
}

//拉取镜像统计
func pullImageCount() {
	pullImageCount := 0
	pullFailCount := 0
	pullSuccessCount := 0
	c := new(sync.Mutex)
	for {
		i := <-countPullChan
		switch i {
		case 0:
			c.Lock()
			pullImageCount += 1
			log.Infof("pullCount:%d\n", pullImageCount)
			if pullImageCount > pullLimit {
				if stopRandTestSignal == 0 {
					//设置停止随机测试标志
					stopRandTestSignal = 1
					//启动goroutine等待仍在工作的goroutine完成
					go checkWorking(2)
				}
				c.Unlock()

				continue
				//	limitChan <- 2
			}
			c.Unlock()
			working += 1
			workingPullChan <- 1
		case 1:
			pullFailCount += 1
			pullLog.Infof("pull[success]:%d,[fail]:%d", pullSuccessCount, pullFailCount)
			working -= 1
		case 2:
			pullSuccessCount += 1
			pullLog.Infof("pull[success]:%d,[fail]:%d", pullSuccessCount, pullFailCount)
			working -= 1
		default:
			log.Errorf("pullImageCount invalid couthNum %d", i)
		}
	}
}

//获取指定长度的随机字符串
func RandStringRunes(n int) string {
	letterRunes := []rune("abcdefghijklmnopqrstuvwxyz")
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func init() {

	rand.Seed(time.Now().Unix())

	flag.StringVar(&appSoarRegistry, "rip", "", "registry ip")
	flag.StringVar(&appSoarPort, "rport", "", "registry port")
	flag.StringVar(&RegisterListenPort, "lport", "", "register server listen port")
	var logFile string
	var pushResult string
	var pullResult string
	flag.StringVar(&logFile, "log", "./autotest.log", "log file path")
	flag.StringVar(&pushResult, "pullResult", "./push.result", "file to record push result")
	flag.StringVar(&pullResult, "pushResult", "./pull.result", "file to record pull result")
	flag.IntVar(&pushLimit, "limit", 0, "number of test image push limit")
	flag.IntVar(&pullLimit, "plimit", 0, "number of test image pull  limit")

	flag.StringVar(&LoginUser, "user", "", "login user")
	flag.StringVar(&LoginPassword, "passwd", "", "login password")
	flag.BoolVar(&Debug, "debug", false, "debug mode")
	flag.Parse()
	if len(appSoarRegistry) == 0 || len(appSoarPort) == 0 || len(RegisterListenPort) == 0 {
		log.Fatal("invalid argument")
	}
	if len(LoginUser) == 0 || len(LoginPassword) == 0 {
		log.Fatal("invalid username Or password")
	}

	LoginServer = appSoarRegistry + ":" + appSoarPort

	if pushLimit == 0 {
		pushLimit = 1000000
	}

	if pullLimit == 0 {
		pullLimit = 1000000
	}
	if Debug {
		log.Level = logrus.DebugLevel
	}

	log.Formatter = &logrus.TextFormatter{DisableColors: true}
	fp, err := os.OpenFile(logFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644) // 让程序关闭后,自动释放
	if err != nil {
		log.Errorf("Create logfile %s fail\n", logFile)
		os.Exit(1)
	}
	log.Out = fp

	pushLog.Formatter = &MyFormatter{}
	fp, err = os.OpenFile(pushResult, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644) // 让程序关闭后,自动释放
	if err != nil {
		log.Errorf("Create logfile %s fail\n", pushResult)
		os.Exit(1)
	}
	pushLog.Out = fp

	pullLog.Formatter = &MyFormatter{}
	fp, err = os.OpenFile(pullResult, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644) // 让程序关闭后,自动释放
	if err != nil {
		log.Errorf("Create logfile %s fail\n", pullResult)
		os.Exit(1)
	}
	pullLog.Out = fp

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
		log.Debugf("ilist :%v\n", i)
		imageMap[i.Name+":"+i.Tag] = i
	}
	for i, j := range imageMap {
		log.Infof("%v:%v\n", i, j)
	}
}

func calCounting() {

	for {
		if working == 0 {
			workingDone <- 1
		}
	}

}

type MyFormatter struct {
}

func (f *MyFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	b := &bytes.Buffer{}
	b.WriteString(entry.Message)
	b.WriteByte('\n')
	return b.Bytes(), nil
}

func main() {
	//	registry healthy check
	log.Debugf("start registry client, healthCheck\n")
	registry := client.NewRegisryClient(appSoarRegistry, appSoarPort)
	go func(r client.RegistryClient) {
		for {
			client.RegistryHealthCheck(r)
			time.Sleep(3 * time.Second)
		}
	}(*registry)

	go pushImageCount()
	go pullImageCount()
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

	for {
		select {
		case _ = <-registry.IllChannel:
			//关闭所有的client
			for _, j := range clientMap {
				j.Shutdown()
			}
			log.Info("registry healthy check fail, exit...")
			os.Exit(1)

		case w := <-limitChan: //1 push 2 pull

			for _, j := range clientMap {
				j.Shutdown()
			}
			switch w {
			case 1:
				log.Infof("push reach limit: %d\n", pushLimit)
				os.Exit(0)
			case 2:
				log.Infof("pull reach limit: %d\n", pullLimit)
				os.Exit(0)
			}
		}
	}
}
