package main

import (
	"bytes"
	"net/http"
	"time"
)

type ClientOpts struct {
	Url       string
	AccessKey string
	SecretKey string
	Timeout   time.Duration
}

type BaseClient struct {
	Opts *ClientOpts
}

type Op struct {
	Name string
}

const (
	DefaultTimeOut time.Duration = time.Second * 10
)

var (
	Get    = Op{Name: "GET"}
	Head   = Op{Name: "HEAD"}
	Delete = Op{Name: "DELETE"}
	Put    = Op{Name: "PUT"}
	Post   = Op{Name: "POST"}
	Update = Op{Name: "UPDATE"}
)

func (c BaseClient) DoAction(path string, op Op) (resp *http.Response, err error) {
	Timeout := c.Opts.Timeout
	if Timeout == 0 {
		Timeout = DefaultTimeOut
	}
	client := &http.Client{Timeout: Timeout}

	req, err := http.NewRequest(op.Name, c.Opts.Url+path, nil)
	if err != nil {
		panic(err.Error())
	}

	req.SetBasicAuth(c.Opts.AccessKey, c.Opts.SecretKey)
	resp, err = client.Do(req)
	return

}

func (c BaseClient) DoPost(path string, data []byte) (resp *http.Response, err error) {
	body := bytes.NewReader(data)

	Timeout := c.Opts.Timeout
	if Timeout == 0 {
		Timeout = DefaultTimeOut
	}
	client := &http.Client{Timeout: Timeout}

	req, err := http.NewRequest("POST", c.Opts.Url+path, body)
	if err != nil {
		panic(err.Error())
	}

	req.SetBasicAuth(c.Opts.AccessKey, c.Opts.SecretKey)
	resp, err = client.Do(req)
	return

}
