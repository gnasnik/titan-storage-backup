package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/docker/go-units"
	"github.com/gnasnik/titan-explorer/core/generated/model"
	logging "github.com/ipfs/go-log/v2"
	"github.com/pkg/errors"
	"github.com/quic-go/quic-go/http3"
	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	dirDateTimeFormat = "20060102"
	maxSingleDirSize  = 18 << 30
	ErrorEventID      = 99
	BackupOutPath     = "/carfile/titan"
	StorageAPI        = "https://api-test1.container1.titannet.io"

	BackupResult = "/v1/storage/backup_result"
	BackupAssets = "/v1/storage/backup_assets"
)

var log = logging.Logger("backup")

var backupInterval = time.Second * 60

type Downloader struct {
	lk         sync.Mutex
	schedulers []*Scheduler

	JobQueue chan *model.Asset
	dirSize  map[string]int64
	token    string
	areaId   string
	running  bool

	etcdClient *EtcdClient

	concurrent      int
	downWorkerQueue chan worker
	dlk             sync.Mutex
	downloading     map[string]struct{}
}

type job func()

type worker struct {
	ID       int
	jobQueue chan job
}

func newDownloader(token string, areaId string, client *EtcdClient, concurrent int) *Downloader {
	schedulers, err := FetchSchedulersFromEtcd(client)
	if err != nil {
		log.Fatalf("fetch scheduler from etcd Failed: %v", err)
	}

	if len(schedulers) == 0 {
		log.Fatal("no scheduler found")
	}

	return &Downloader{
		JobQueue:   make(chan *model.Asset, 1),
		dirSize:    make(map[string]int64),
		schedulers: schedulers,
		areaId:     areaId,
		token:      token,
		etcdClient: client,

		downWorkerQueue: make(chan worker, concurrent),
		concurrent:      concurrent,
		downloading:     make(map[string]struct{}),
	}
}

func (d *Downloader) Push(jobs []*model.Asset) {
	//d.lk.Lock()
	//defer d.lk.Unlock()

	for _, j := range jobs {
		d.JobQueue <- j
	}

	d.running = false
}

func (d *Downloader) create(ctx context.Context, job *model.Asset) (*model.Asset, error) {
	dir := job.EndTime.Format(dirDateTimeFormat)

	outPath, err := d.getOutPath(dir)
	if err != nil {
		return nil, err
	}

	var s *Scheduler

	for _, sd := range d.schedulers {
		if sd.AreaId == d.areaId {
			s = sd
			break
		}
	}

	if s == nil {
		return nil, errors.New("no scheduler found")
	}

	err = d.download(ctx, s, outPath, job.Cid, job.TotalSize)
	if err != nil {
		log.Errorf("download CARFile %s: %v", job.Cid, err)
		job.Event = ErrorEventID
		return job, err
	}

	job.Path = outPath
	return job, nil
}

func (d *Downloader) GetScheduler(areaId string) *Scheduler {
	for _, s := range d.schedulers {
		if s.AreaId == areaId {
			return s
		}
	}
	return nil
}

func (d *Downloader) download(ctx context.Context, scheduler *Scheduler, outPath, cid string, size int64) error {
	downloadInfos, err := scheduler.Api.GetAssetSourceDownloadInfo(ctx, cid)
	if err != nil {
		log.Errorf("GetAssetSourceDownloadInfo: %v", err)
		return err
	}

	if len(downloadInfos.SourceList) == 0 {
		return errors.New(fmt.Sprintf("CARFile %s not found", cid))
	}

	start := time.Now()
	hrs := units.BytesSize(float64(size))

	for _, downloadInfo := range downloadInfos.SourceList {
		reader, err := request(downloadInfo.Address, cid, downloadInfo.Tk)
		if err != nil {
			log.Errorf("download requeset: %v", err)
			continue
		}

		file, err := os.Create(filepath.Join(outPath, cid+".car"))
		if err != nil {
			return err
		}

		_, err = io.Copy(file, reader)
		if err != nil {
			return err
		}

		d.lk.Lock()
		d.dirSize[outPath] += size
		d.lk.Unlock()

		log.Infof("Successfully download CARFile %s, size: %s, cost: %v.\n", outPath, hrs, time.Since(start))
		return nil
	}

	return nil
}

func (d *Downloader) async() {
	ticker := time.NewTicker(backupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if d.running {
				log.Infof("backup processing...")
				continue
			}

			d.running = true

			assets, err := getJobs()
			if err != nil {
				log.Errorf("get jobs: %v", err)
				continue
			}

			log.Infof("fetch %d jobs", len(assets))

			if len(assets) == 0 {
				d.running = false
				continue
			}

			d.Push(assets)
			ticker.Reset(backupInterval)
		}
	}

}

func (d *Downloader) run() {
	d.initDownWorker()

	for {

		log.Infof("current worker queue: %d, job queue: %d", len(d.downWorkerQueue), len(d.JobQueue))

		// get asset to download
		asset := <-d.JobQueue

		select {
		case wrk := <-d.downWorkerQueue:
			go func(a *model.Asset, w worker) {
				// push job queue
				jobFunc := d.jobProcess(a)
				jobFunc()
				// push back worker queue
				d.downWorkerQueue <- w
			}(asset, wrk)
		}
	}
}

func (d *Downloader) jobProcess(asset *model.Asset) job {
	return func() {
		d.dlk.Lock()
		if _, existing := d.downloading[asset.Cid]; existing {
			log.Infof("cid %s is downloading...", asset.Cid)
			d.dlk.Unlock()
			return
		}
		d.downloading[asset.Cid] = struct{}{}
		d.dlk.Unlock()

		j, err := d.create(context.Background(), asset)
		if err != nil {
			log.Errorf("download: %v", err)
		}

		if err == nil && j != nil {
			log.Infof("process job: %s event: %d, path: %s", j.Cid, j.Event, j.Path)
		}

		err = pushResult(d.token, []*model.Asset{asset})
		if err != nil {
			log.Errorf("push result: %v", err)
		}

		time.Sleep(time.Second)

		d.dlk.Lock()
		delete(d.downloading, asset.Cid)
		d.dlk.Unlock()
	}
}

func (d *Downloader) initDownWorker() {
	for i := 0; i < d.concurrent; i++ {
		d.downWorkerQueue <- worker{
			ID:       i,
			jobQueue: make(chan job, 1),
		}
	}
}

func (d *Downloader) createOrGetSize(dir string) (int64, error) {
	if !fileutil.Exist(dir) {
		return 0, os.Mkdir(dir, 0775)
	}

	d.lk.Lock()
	defer d.lk.Unlock()

	if size, ok := d.dirSize[dir]; ok {
		return size, nil
	}

	size, err := getDirSize(dir)
	if err != nil {
		return 0, err
	}
	d.dirSize[dir] = size

	return size, nil
}

func getDirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	return size, err
}

func (d *Downloader) getOutPath(dir string) (string, error) {
	var outPath string

	for c := 'a'; c < 'z'; c++ {
		outPath = filepath.Join(BackupOutPath, fmt.Sprintf("%s%c", dir, c))
		size, err := d.createOrGetSize(outPath)
		if err != nil {
			log.Errorf("createOrGetSize %s: %v", dir, err)
			return "", err
		}

		if size < maxSingleDirSize {
			break
		}
	}

	return outPath, nil
}

func request(url, cid string, token *types.Token) (io.ReadCloser, error) {
	var scheme string
	if !strings.HasPrefix(url, "http") {
		scheme = "https://"
	}

	endpoint := fmt.Sprintf("%s%s/ipfs/%s?format=car", scheme, url, cid)

	log.Infof("downloading from endpoint: %s", endpoint)

	req, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}

	//req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", start, end))

	client := http.Client{
		Timeout: 30 * time.Minute,
		Transport: &http3.RoundTripper{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("http request: %d %v", resp.StatusCode, resp.Status)
	}

	return resp.Body, err
}

func pushResult(token string, jobs []*model.Asset) error {
	data, err := json.Marshal(jobs)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s%s", StorageAPI, BackupResult)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(data))
	if err != nil {
		return err
	}

	req.Header.Add("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status: %d %v", resp.StatusCode, resp.Status)
	}

	log.Infof("Successfully updated backup result")
	return nil
}

type getJobResp struct {
	Code int
	Data interface{}
}

func getJobs() ([]*model.Asset, error) {
	url := fmt.Sprintf("%s%s", StorageAPI, BackupAssets)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status: %d %v", resp.StatusCode, resp.Status)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var ret getJobResp
	err = json.Unmarshal(data, &ret)
	if err != nil {
		return nil, err
	}

	data, err = json.Marshal(ret.Data)
	if err != nil {
		return nil, err
	}

	var out struct {
		List  []*model.Asset `json:"list"`
		Total int            `json:"total"`
	}

	err = json.Unmarshal(data, &out)
	if err != nil {
		return nil, err
	}

	return out.List, nil
}
