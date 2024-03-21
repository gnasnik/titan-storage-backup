package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/gnasnik/titan-explorer/core/generated/model"
	"github.com/gnasnik/titan-explorer/core/statistics"
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
	StorageAPI        = "https://api-storage.container1.titannet.io"
	BackupResult      = "/v1/storage/backup_result"
	BackupAssets      = "/v1/storage/backup_assets"
)

var log = logging.Logger("backup")

var backupInterval = time.Minute * 10

type Downloader struct {
	lk         sync.Mutex
	schedulers []*statistics.Scheduler

	JobQueue chan []*model.Asset
	dirSize  map[string]int64
	token    string
	running  bool

	etcdClient *statistics.EtcdClient
}

func newDownloader(token string, client *statistics.EtcdClient) *Downloader {
	// 从 Etcd 上获取所有调度器的配置
	schedulers, err := statistics.FetchSchedulersFromEtcd(client)
	if err != nil {
		log.Fatalf("fetch scheduler from etcd Failed: %v", err)
	}

	if len(schedulers) == 0 {
		log.Fatal("no scheduler found")
	}

	return &Downloader{
		JobQueue:   make(chan []*model.Asset, 1),
		dirSize:    make(map[string]int64),
		schedulers: schedulers,
		token:      token,
		etcdClient: client,
	}
}

func (d *Downloader) Push(jobs []*model.Asset) {
	d.lk.Lock()
	defer d.lk.Unlock()

	d.JobQueue <- jobs
}

func (d *Downloader) create(ctx context.Context, job *model.Asset) (*model.Asset, error) {
	dir := job.EndTime.Format(dirDateTimeFormat)

	outPath, err := d.getOutPath(dir)
	if err != nil {
		return nil, err
	}

	err = d.download(ctx, outPath, job.Cid, job.TotalSize)
	if err != nil {
		log.Errorf("download CARFile %s: %v", job.Cid, err)
		job.Event = ErrorEventID
		return job, err
	}

	job.Path = outPath
	return job, nil
}

func (d *Downloader) download(ctx context.Context, outPath, cid string, size int64) error {
	var outErr error

	for _, scheduler := range d.schedulers {
		downloadInfos, err := scheduler.Api.GetCandidateDownloadInfos(ctx, cid)
		if err != nil {
			log.Errorf("GetCandidateDownloadInfos: %v", err)
			outErr = err
			continue
		}

		if len(downloadInfos) == 0 {
			outErr = errors.New(fmt.Sprintf("CARFile %s not found", cid))
			continue
		}

		for _, downloadInfo := range downloadInfos {
			reader, err := request(downloadInfo.Address, cid, downloadInfo.Tk)
			if err != nil {
				log.Errorf("download requeset: %v", err)
				outErr = err
				continue
			}

			file, err := os.Create(filepath.Join(outPath, cid+".car"))
			if err != nil {
				outErr = err
				return err
			}

			_, err = io.Copy(file, reader)
			if err != nil {
				outErr = err
				return err
			}

			d.lk.Lock()
			d.dirSize[outPath] += size
			d.lk.Unlock()

			outErr = nil

			log.Infof("Successfully download CARFile %s.\n", outPath)
			return nil
		}
	}

	return outErr
}

func (d *Downloader) async() {
	ticker := time.NewTicker(backupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if d.running {
				log.Errorf("backup processing...")
				continue
			}

			d.running = true

			assets, err := getJobs()
			if err != nil {
				log.Errorf("get jobs: %v", err)
				continue
			}

			if len(assets) == 0 {
				continue
			}

			log.Infof("fetch %d jobs", len(assets))

			d.Push(assets)
			ticker.Reset(backupInterval)
		}
	}

}

func (d *Downloader) run() {
	for {
		select {
		case jobs := <-d.JobQueue:

			for _, job := range jobs {
				j, err := d.create(context.Background(), job)
				if err != nil {
					log.Errorf("download: %v", err)
				}

				log.Infof("process job: %s event: %d, path: %s", j.Cid, j.Event, j.Path)

				err = pushResult(d.token, []*model.Asset{job})
				if err != nil {
					log.Errorf("push result: %v", err)
				}
			}

			d.running = false

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

	log.Debugf("endpoint: %s", endpoint)

	req, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}

	//req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", start, end))

	client := http.Client{
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
