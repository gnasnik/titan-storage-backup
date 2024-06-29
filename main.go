package main

import (
	"flag"
	logging "github.com/ipfs/go-log/v2"
	"strings"
)

var (
	etcd     string
	user     string
	password string
	token    string
	areaId   string
)

func init() {
	flag.StringVar(&etcd, "etcd", "", "etcd address")
	flag.StringVar(&user, "user", "", "etcd user")
	flag.StringVar(&password, "password", "", "etcd password")
	flag.StringVar(&token, "token", "", "storage api authenticate token")
	flag.StringVar(&areaId, "area_id", "", "scheduler area id")
}

func main() {
	flag.Parse()

	logging.SetDebugLogging()

	addresses := strings.Split(etcd, ",")
	client, err := NewEtcdClient(addresses)
	if err != nil {
		log.Fatal("New etcdClient Failed: %v", err)
	}

	downloader := newDownloader(token, areaId, client, 5)
	go downloader.async()

	log.Infof("Started")
	downloader.run()
}
