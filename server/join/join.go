// Copyright 2016 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package join

import (
	"fmt"
	"os"
	"path"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/etcdutil"
	"github.com/tikv/pd/server/config"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/embed"
	"go.uber.org/zap"
)

const (
	// privateFileMode grants owner to read/write a file.
	privateFileMode = 0600
	// privateDirMode grants owner to make/remove files inside the directory.
	privateDirMode = 0700
)

// listMemberRetryTimes is the retry times of list member.
var listMemberRetryTimes = 20

// PrepareJoinCluster sends MemberAdd command to PD cluster,
// and returns the initial configuration of the PD cluster.
//
// TL;TR: The join functionality is safe. With data, join does nothing, w/o data
//
//	and it is not a member of cluster, join does MemberAdd, it returns an
//	error if PD tries to join itself, missing data or join a duplicated PD.
//
// Etcd automatically re-joins the cluster if there is a data directory. So
// first it checks if there is a data directory or not. If there is, it returns
// an empty string (etcd will get the correct configurations from the data
// directory.)
//
// If there is no data directory, there are following cases:
//
//   - A new PD joins an existing cluster.
//     What join does: MemberAdd, MemberList, then generate initial-cluster.
//
//   - A failed PD re-joins the previous cluster.
//     What join does: return an error. (etcd reports: raft log corrupted,
//     truncated, or lost?)
//
//   - A deleted PD joins to previous cluster.
//     What join does: MemberAdd, MemberList, then generate initial-cluster.
//     (it is not in the member list and there is no data, so
//     we can treat it as a new PD.)
//
// If there is a data directory, there are following special cases:
//
//   - A failed PD tries to join the previous cluster but it has been deleted
//     during its downtime.
//     What join does: return "" (etcd will connect to other peers and find
//     that the PD itself has been removed.)
//
//   - A deleted PD joins the previous cluster.
//     What join does: return "" (as etcd will read data directory and find
//     that the PD itself has been removed, so an empty string
//     is fine.)
func PrepareJoinCluster(cfg *config.Config) error {
	// - A PD tries to join itself.
	if cfg.Join == "" {
		return nil
	}

	if cfg.Join == cfg.AdvertiseClientUrls {
		return errors.New("join self is forbidden")
	}

	log.Info("prepare join cluster",
		zap.String("name", cfg.Name),
		zap.String("join", cfg.Join),
	)

	filePath := path.Join(cfg.DataDir, "join")
	// Read the persist join config
	if _, err := os.Stat(filePath); !os.IsNotExist(err) {
		s, err := os.ReadFile(filePath)
		if err != nil {
			log.Fatal("read the join config meet error", errs.ZapError(errs.ErrIORead, err))
		}
		cfg.InitialCluster = strings.TrimSpace(string(s))
		cfg.InitialClusterState = embed.ClusterStateFlagExisting
		return nil
	}

	initialCluster := ""
	// Cases with data directory.
	if isDataExist(path.Join(cfg.DataDir, "member")) {
		cfg.InitialCluster = initialCluster
		cfg.InitialClusterState = embed.ClusterStateFlagExisting
		return nil
	}

	// Below are cases without data directory.
	tlsConfig, err := cfg.Security.ToTLSConfig()
	if err != nil {
		return err
	}
	lgc := zap.NewProductionConfig()
	lgc.Encoding = log.ZapEncodingName
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(cfg.Join, ","),
		DialTimeout: etcdutil.DefaultDialTimeout,
		TLS:         tlsConfig,
		LogConfig:   &lgc,
	})
	if err != nil {
		return errors.WithStack(err)
	}
	defer client.Close()

	// lock for pd join
	session, err := concurrency.NewSession(client)
	if err != nil {
		return err
	}
	defer session.Close()

	locker := concurrency.NewMutex(session, "/pd/join")
	err = locker.Lock(client.Ctx())
	if err != nil {
		return nil
	}
	err = prepareJoinClusterLocked(cfg, client, filePath)
	locker.Unlock(client.Ctx())

	return err
}

func prepareJoinClusterLocked(cfg *config.Config, client *clientv3.Client, filePath string) error {
	listResp, err := etcdutil.ListEtcdMembers(client)
	if err != nil {
		return err
	}

	needWaiting := false
	for {
		if needWaiting {
			time.Sleep(500 * time.Millisecond)
			listResp, err = etcdutil.ListEtcdMembers(client)
			if err == nil {
				needWaiting = false
			} else {
				log.Info("list etcd member fail", zap.Error(err))
				continue
			}
		}
		for _, m := range listResp.Members {
			if len(m.Name) == 0 {
				peerUrls := strings.Split(cfg.AdvertisePeerUrls, ",")
				slices.Sort(peerUrls)
				slices.Sort(m.PeerURLs)
				if !reflect.DeepEqual(peerUrls, m.PeerURLs) {
					log.Info("there is a member that has not joined successfully",
						zap.Strings("client urls", m.ClientURLs),
						zap.Strings("peer urls", m.PeerURLs),
					)
					needWaiting = true
				}
			}
		}
		if !needWaiting {
			break
		}
	}

	memberCount := 0
	existed := false
	missingData := false
	memberID := uint64(0)
	for _, m := range listResp.Members {
		if m.Name == cfg.Name {
			peerUrls := strings.Split(cfg.AdvertisePeerUrls, ",")
			slices.Sort(peerUrls)
			slices.Sort(m.PeerURLs)
			log.Info("found member",
				zap.String("name", m.Name),
				zap.Strings("client urls", m.ClientURLs),
				zap.Strings("peer urls", m.PeerURLs),
			)
			if reflect.DeepEqual(peerUrls, m.PeerURLs) {
				missingData = true
				memberID = m.ID
			} else {
				existed = true
			}
		} else if len(m.Name) == 0 {
			peerUrls := strings.Split(cfg.AdvertisePeerUrls, ",")
			slices.Sort(peerUrls)
			slices.Sort(m.PeerURLs)
			log.Info("empty member",
				zap.Strings("client urls", m.ClientURLs),
				zap.Strings("peer urls", m.PeerURLs),
			)
			if reflect.DeepEqual(peerUrls, m.PeerURLs) {
				missingData = true
				memberID = m.ID
			} else {
				existed = true
			}
		}
		if !m.IsLearner {
			memberCount++
		}
	}

	log.Info("list etcd member info",
		zap.Int("total count", len(listResp.Members)),
		zap.Int("member count", memberCount),
		zap.Bool("existed", existed),
		zap.Bool("missing data", missingData),
	)

	// - A failed PD re-joins the previous cluster.
	if existed {
		return errors.New("missing data or join a duplicated pd")
	}

	var addResp *clientv3.MemberAddResponse

	failpoint.Inject("add-member-failed", func() {
		listMemberRetryTimes = 2
		failpoint.Goto("LabelSkipAddMember")
	})
	// - A new PD joins an existing cluster.
	// - A deleted PD joins to previous cluster.
	if missingData {
		_, err := etcdutil.RemoveEtcdMember(client, memberID)
		if err != nil {
			log.Warn("del etcd member fail",
				zap.Uint64("id", memberID),
				zap.Error(err),
			)
			return errors.New("missing data or join a duplicated pd")
		}
	}

	for {
		addResp, err = etcdutil.AddEtcdLearner(client, []string{cfg.AdvertisePeerUrls})
		if err != nil {
			log.Warn("add etcd learner fail", zap.Error(err))
			time.Sleep(1000 * time.Millisecond)
		} else {
			break
		}
	}

	failpoint.Label("LabelSkipAddMember")

	var (
		pds      []string
		listSucc bool
	)

	for i := 0; i < listMemberRetryTimes; i++ {
		log.Info("check add etcd member result")

		listResp, err = etcdutil.ListEtcdMembers(client)
		if err != nil {
			return err
		}

		pds = []string{}
		for _, memb := range listResp.Members {
			n := memb.Name
			if addResp != nil && memb.ID == addResp.Member.ID {
				n = cfg.Name
				listSucc = true
			}
			if len(n) == 0 {
				peerUrls := strings.Split(cfg.AdvertisePeerUrls, ",")
				slices.Sort(peerUrls)
				slices.Sort(memb.PeerURLs)
				if !reflect.DeepEqual(peerUrls, memb.PeerURLs) {
					return errors.New("there is a member that has not joined successfully")
				}
			}
			for _, m := range memb.PeerURLs {
				pds = append(pds, fmt.Sprintf("%s=%s", n, m))
			}
		}

		if listSucc {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !listSucc {
		return errors.Errorf("join failed, adds the new member %s may failed", cfg.Name)
	}

	initialCluster := strings.Join(pds, ",")
	log.Info("save initial cluster info",
		zap.String("join", initialCluster))
	cfg.InitialCluster = initialCluster
	cfg.InitialClusterState = embed.ClusterStateFlagExisting
	err = os.MkdirAll(cfg.DataDir, privateDirMode)
	if err != nil && !os.IsExist(err) {
		return errors.WithStack(err)
	}

	err = os.WriteFile(filePath, []byte(cfg.InitialCluster), privateFileMode)
	return errors.WithStack(err)
}

func isDataExist(d string) bool {
	dir, err := os.Open(d)
	if err != nil {
		log.Info("failed to open directory, maybe start for the first time", errs.ZapError(err))
		return false
	}
	defer func() {
		if err := dir.Close(); err != nil {
			log.Error("failed to close file", errs.ZapError(err))
		}
	}()

	names, err := dir.Readdirnames(-1)
	if err != nil {
		log.Error("failed to list directory", errs.ZapError(errs.ErrReadDirName, err))
		return false
	}
	return len(names) != 0
}
