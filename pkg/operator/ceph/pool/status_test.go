/*
Copyright 2020 The Rook Authors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package pool to manage a rook pool.
package pool

import (
	"fmt"
	"testing"

	"github.com/pkg/errors"
	cephv1 "github.com/rook/rook/pkg/apis/ceph.rook.io/v1"
	"github.com/rook/rook/pkg/clusterd"
	cephclient "github.com/rook/rook/pkg/daemon/ceph/client"
	opcontroller "github.com/rook/rook/pkg/operator/ceph/controller"
	exectest "github.com/rook/rook/pkg/util/exec/test"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestUpdateStatusInfo(t *testing.T) {
	cephBlockPoolReplicated := &cephv1.CephBlockPool{
		Spec: cephv1.NamedBlockPoolSpec{
			Name: "test-pool-replicated",
			PoolSpec: cephv1.PoolSpec{
				Replicated: cephv1.ReplicatedSpec{
					Size: 3,
				},
			},
		},
		Status: &cephv1.CephBlockPoolStatus{
			Phase: cephv1.ConditionProgressing,
		},
	}
	updateStatusInfo(cephBlockPoolReplicated, "")
	statusInfo := cephBlockPoolReplicated.Status.Info
	assert.Equal(t, "Replicated", statusInfo["type"])
	assert.Equal(t, cephv1.DefaultFailureDomain, statusInfo["failureDomain"])
	assert.Empty(t, statusInfo[opcontroller.RBDMirrorBootstrapPeerSecretName])

	cephBlockPoolErasureCoded := &cephv1.CephBlockPool{
		Spec: cephv1.NamedBlockPoolSpec{
			Name: "test-pool-erasure-coded",
			PoolSpec: cephv1.PoolSpec{
				FailureDomain: "osd",
				ErasureCoded: cephv1.ErasureCodedSpec{
					CodingChunks: 6,
					DataChunks:   2,
				},
			},
		},
		Status: &cephv1.CephBlockPoolStatus{
			Phase: cephv1.ConditionProgressing,
		},
	}
	updateStatusInfo(cephBlockPoolErasureCoded, "")
	statusInfo = cephBlockPoolErasureCoded.Status.Info
	assert.Equal(t, "Erasure Coded", statusInfo["type"])
	assert.Equal(t, "osd", statusInfo["failureDomain"])
	assert.Empty(t, statusInfo[opcontroller.RBDMirrorBootstrapPeerSecretName])

	cephBlockPoolErasureCoded.Spec.PoolSpec.Mirroring = cephv1.MirroringSpec{
		Enabled: true,
	}

	updateStatusInfo(cephBlockPoolErasureCoded, "")
	statusInfo = cephBlockPoolErasureCoded.Status.Info
	assert.Empty(t, statusInfo[opcontroller.RBDMirrorBootstrapPeerSecretName])

	cephBlockPoolErasureCoded.Status.Phase = cephv1.ConditionReady
	updateStatusInfo(cephBlockPoolErasureCoded, "")
	statusInfo = cephBlockPoolErasureCoded.Status.Info
	assert.NotEmpty(t, statusInfo[opcontroller.RBDMirrorBootstrapPeerSecretName])
}

func TestUpdateStatusInfoPrimaryFailureDomain(t *testing.T) {
	cephBlockPool := &cephv1.CephBlockPool{
		Spec: cephv1.NamedBlockPoolSpec{
			PoolSpec: cephv1.PoolSpec{
				FailureDomain: "zone",
				Replicated:    cephv1.ReplicatedSpec{Size: 3},
			},
		},
		Status: &cephv1.CephBlockPoolStatus{Phase: cephv1.ConditionReady},
	}

	updateStatusInfo(cephBlockPool, "a")
	assert.Equal(t, "a", cephBlockPool.Status.Info["primaryFailureDomain"])

	updateStatusInfo(cephBlockPool, "")
	_, ok := cephBlockPool.Status.Info["primaryFailureDomain"]
	assert.False(t, ok)
}

func TestWriteAffinityPrimary(t *testing.T) {
	cephBlockPool := &cephv1.CephBlockPool{
		ObjectMeta: metav1.ObjectMeta{Name: "mypool"},
		Spec: cephv1.NamedBlockPoolSpec{
			PoolSpec: cephv1.PoolSpec{
				FailureDomain: "zone",
				Replicated: cephv1.ReplicatedSpec{
					Size:          3,
					WriteAffinity: &cephv1.WriteAffinitySpec{FailureDomains: []string{"zone.a", "b", "c"}},
				},
			},
		},
		Status: &cephv1.CephBlockPoolStatus{Info: map[string]string{"primaryFailureDomain": "zone.a"}},
	}

	crushRule := "mypool_wa_0123abcd"
	firstTake := "zone-a~ssd"
	var poolErr error
	executor := &exectest.MockExecutor{
		MockExecuteCommandWithOutput: func(command string, args ...string) (string, error) {
			if args[0] == "osd" && args[1] == "pool" && args[2] == "get" {
				return fmt.Sprintf(`{"pool":"mypool","crush_rule":%q}`, crushRule), poolErr
			}
			if args[0] == "osd" && args[1] == "crush" && args[2] == "rule" && args[3] == "dump" {
				return fmt.Sprintf(`{"rule_name":%q,"steps":[{"op":"take","item_name":%q},{"op":"chooseleaf_firstn","num":1,"type":"host"},{"op":"emit"}]}`, crushRule, firstTake), nil
			}
			return "", errors.Errorf("unexpected ceph command %q", args)
		},
	}
	r := &ReconcileCephBlockPool{context: &clusterd.Context{Executor: executor}, clusterInfo: cephclient.AdminTestClusterInfo("mycluster")}

	t.Run("rule places primaries in the first failure domain", func(t *testing.T) {
		assert.Equal(t, "zone.a", r.writeAffinityPrimary(cephBlockPool))
	})

	t.Run("rule places primaries elsewhere", func(t *testing.T) {
		firstTake = "b"
		defer func() { firstTake = "zone-a~ssd" }()
		assert.Equal(t, "", r.writeAffinityPrimary(cephBlockPool))
	})

	t.Run("pool does not use a write affinity rule", func(t *testing.T) {
		crushRule = "mypool"
		defer func() { crushRule = "mypool_wa_0123abcd" }()
		assert.Equal(t, "", r.writeAffinityPrimary(cephBlockPool))
	})

	t.Run("keeps the last value on error", func(t *testing.T) {
		poolErr = errors.New("timeout")
		defer func() { poolErr = nil }()
		assert.Equal(t, "zone.a", r.writeAffinityPrimary(cephBlockPool))
	})

	t.Run("not a write affinity pool", func(t *testing.T) {
		p := cephBlockPool.DeepCopy()
		p.Spec.Replicated.WriteAffinity = nil
		assert.Equal(t, "", r.writeAffinityPrimary(p))
	})
}
