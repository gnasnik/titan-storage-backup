package main

import (
	"context"
	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/client"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/lib/etcdcli"
	"net/http"
	"strings"
)

type Scheduler struct {
	Uuid   string
	AreaId string
	Api    api.Scheduler
	Closer func()
}

type EtcdClient struct {
	cli *etcdcli.Client
	// key is etcd key, value is types.SchedulerCfg pointer
	configMap map[string]*types.SchedulerCfg
}

func NewEtcdClient(addresses []string) (*EtcdClient, error) {
	etcd, err := etcdcli.New(addresses)
	if err != nil {
		return nil, err
	}

	etcdClient := &EtcdClient{
		cli: etcd,
		//schedulerConfigs: make(map[string][]*types.SchedulerCfg),
		configMap: make(map[string]*types.SchedulerCfg),
	}

	//if err := ec.loadSchedulerConfigs(); err != nil {
	//	return nil, err
	//}

	return etcdClient, nil
}

func (ec *EtcdClient) loadSchedulerConfigs() (map[string][]*types.SchedulerCfg, error) {
	resp, err := ec.cli.GetServers(types.NodeScheduler.String())
	if err != nil {
		return nil, err
	}

	schedulerConfigs := make(map[string][]*types.SchedulerCfg)

	for _, kv := range resp.Kvs {
		var configScheduler *types.SchedulerCfg
		err := etcdcli.SCUnmarshal(kv.Value, &configScheduler)
		if err != nil {
			return nil, err
		}
		configs, ok := schedulerConfigs[configScheduler.AreaID]
		if !ok {
			configs = make([]*types.SchedulerCfg, 0)
		}
		configs = append(configs, configScheduler)

		schedulerConfigs[configScheduler.AreaID] = configs
		ec.configMap[string(kv.Key)] = configScheduler
	}

	//ec.schedulerConfigs = schedulerConfigs
	return schedulerConfigs, nil
}

func FetchSchedulersFromEtcd(etcdClient *EtcdClient) ([]*Scheduler, error) {
	schedulerConfigs, err := etcdClient.loadSchedulerConfigs()
	if err != nil {
		log.Errorf("load scheduer from etcd: %v", err)
		return nil, err
	}

	var out []*Scheduler

	for key, schedulerURLs := range schedulerConfigs {
		for _, SchedulerCfg := range schedulerURLs {
			// https protocol still in test, we use http for now.
			schedulerURL := strings.Replace(SchedulerCfg.SchedulerURL, "https", "http", 1)
			headers := http.Header{}
			headers.Add("Authorization", "Bearer "+SchedulerCfg.AccessToken)
			clientInit, closeScheduler, err := client.NewScheduler(context.Background(), schedulerURL, headers)
			if err != nil {
				log.Errorf("create scheduler rpc client: %v", err)
			}
			out = append(out, &Scheduler{
				Uuid:   schedulerURL,
				Api:    clientInit,
				AreaId: key,
				Closer: closeScheduler,
			})
			//schedulerApi = clientInit
		}
	}

	log.Infof("fetch %d schedulers from Etcd", len(out))

	return out, nil
}
