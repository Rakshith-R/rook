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
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/pkg/errors"
	cephv1 "github.com/rook/rook/pkg/apis/ceph.rook.io/v1"
	"github.com/rook/rook/pkg/clusterd"
	cephclient "github.com/rook/rook/pkg/daemon/ceph/client"
	exectest "github.com/rook/rook/pkg/util/exec/test"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestValidatePool(t *testing.T) {
	context := &clusterd.Context{Executor: &exectest.MockExecutor{}}
	clusterInfo := &cephclient.ClusterInfo{Namespace: "myns"}
	clusterSpec := &cephv1.ClusterSpec{}

	t.Run("not specifying replication or EC settings is invalid", func(t *testing.T) {
		p := cephv1.CephBlockPool{ObjectMeta: metav1.ObjectMeta{Name: "mypool", Namespace: clusterInfo.Namespace}}
		err := validatePool(context, clusterInfo, clusterSpec, &p)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "either of erasurecoded or replicated fields should be set")
	})

	t.Run("must specify name", func(t *testing.T) {
		p := cephv1.CephBlockPool{ObjectMeta: metav1.ObjectMeta{Namespace: clusterInfo.Namespace}}
		p.Spec.Replicated.Size = 3
		err := validatePool(context, clusterInfo, clusterSpec, &p)
		assert.Error(t, err)
	})

	t.Run("must specify namespace", func(t *testing.T) {
		p := cephv1.CephBlockPool{ObjectMeta: metav1.ObjectMeta{Name: "mypool"}}
		p.Spec.Replicated.Size = 3
		err := validatePool(context, clusterInfo, clusterSpec, &p)
		assert.Error(t, err)
	})

	t.Run("must not specify both replication and EC settings", func(t *testing.T) {
		p := cephv1.CephBlockPool{ObjectMeta: metav1.ObjectMeta{Name: "mypool", Namespace: clusterInfo.Namespace}}
		p.Spec.Replicated.Size = 1
		p.Spec.Replicated.RequireSafeReplicaSize = false
		p.Spec.ErasureCoded.CodingChunks = 2
		p.Spec.ErasureCoded.DataChunks = 3
		err := validatePool(context, clusterInfo, clusterSpec, &p)
		assert.Error(t, err)
	})

	t.Run("succeed with replication settings", func(t *testing.T) {
		p := cephv1.CephBlockPool{ObjectMeta: metav1.ObjectMeta{Name: "mypool", Namespace: clusterInfo.Namespace}}
		p.Spec.Replicated.Size = 1
		p.Spec.Replicated.RequireSafeReplicaSize = false
		err := validatePool(context, clusterInfo, clusterSpec, &p)
		assert.NoError(t, err)
	})

	t.Run("size is 1 and RequireSafeReplicaSize is true", func(t *testing.T) {
		p := cephv1.CephBlockPool{ObjectMeta: metav1.ObjectMeta{Name: "mypool", Namespace: clusterInfo.Namespace}}
		p.Spec.Replicated.Size = 1
		p.Spec.Replicated.RequireSafeReplicaSize = true
		err := validatePool(context, clusterInfo, clusterSpec, &p)
		assert.Error(t, err)
	})

	t.Run("succeed with ec settings", func(t *testing.T) {
		p := cephv1.CephBlockPool{ObjectMeta: metav1.ObjectMeta{Name: "mypool", Namespace: clusterInfo.Namespace}}
		p.Spec.ErasureCoded.CodingChunks = 1
		p.Spec.ErasureCoded.DataChunks = 2
		err := validatePool(context, clusterInfo, clusterSpec, &p)
		assert.NoError(t, err)
	})

	t.Run("fail Parameters['compression_mode'] is unknown", func(t *testing.T) {
		p := cephv1.CephBlockPool{ObjectMeta: metav1.ObjectMeta{Name: "mypool", Namespace: clusterInfo.Namespace}}
		p.Spec.Replicated.Size = 1
		p.Spec.Replicated.RequireSafeReplicaSize = false
		p.Spec.Parameters = map[string]string{"compression_mode": "foo"}
		err := validatePool(context, clusterInfo, clusterSpec, &p)
		assert.Error(t, err)
		assert.EqualError(t, err, "failed to validate pool spec unknown compression mode \"foo\"")
		assert.Equal(t, "foo", p.Spec.Parameters["compression_mode"])
	})

	t.Run("success Parameters['compression_mode'] is known", func(t *testing.T) {
		p := cephv1.CephBlockPool{ObjectMeta: metav1.ObjectMeta{Name: "mypool", Namespace: clusterInfo.Namespace}}
		p.Spec.Replicated.Size = 1
		p.Spec.Replicated.RequireSafeReplicaSize = false
		p.Spec.Parameters = map[string]string{"compression_mode": "aggressive"}
		err := validatePool(context, clusterInfo, clusterSpec, &p)
		assert.NoError(t, err)
	})

	t.Run("fail since replica size is lower than ReplicasPerFailureDomain", func(t *testing.T) {
		p := cephv1.CephBlockPool{ObjectMeta: metav1.ObjectMeta{Name: "mypool", Namespace: clusterInfo.Namespace}}
		p.Spec.Replicated.Size = 1
		p.Spec.Replicated.ReplicasPerFailureDomain = 2
		err := validatePool(context, clusterInfo, clusterSpec, &p)
		assert.Error(t, err)
	})

	t.Run("fail since replica size is equal than ReplicasPerFailureDomain", func(t *testing.T) {
		p := cephv1.CephBlockPool{ObjectMeta: metav1.ObjectMeta{Name: "mypool", Namespace: clusterInfo.Namespace}}
		p.Spec.Replicated.Size = 2
		p.Spec.Replicated.ReplicasPerFailureDomain = 2
		err := validatePool(context, clusterInfo, clusterSpec, &p)
		assert.Error(t, err)
	})

	t.Run("fail since ReplicasPerFailureDomain is not a power of 2", func(t *testing.T) {
		p := cephv1.CephBlockPool{ObjectMeta: metav1.ObjectMeta{Name: "mypool", Namespace: clusterInfo.Namespace}}
		p.Spec.Replicated.Size = 4
		p.Spec.Replicated.ReplicasPerFailureDomain = 3
		err := validatePool(context, clusterInfo, clusterSpec, &p)
		assert.Error(t, err)
	})

	t.Run("fail since ReplicasPerFailureDomain is not a power of 2", func(t *testing.T) {
		p := cephv1.CephBlockPool{ObjectMeta: metav1.ObjectMeta{Name: "mypool", Namespace: clusterInfo.Namespace}}
		p.Spec.Replicated.Size = 4
		p.Spec.Replicated.ReplicasPerFailureDomain = 5
		err := validatePool(context, clusterInfo, clusterSpec, &p)
		assert.Error(t, err)
	})

	t.Run("failure the sub domain does not exist", func(t *testing.T) {
		p := cephv1.CephBlockPool{ObjectMeta: metav1.ObjectMeta{Name: "mypool", Namespace: clusterInfo.Namespace}}
		p.Spec.Replicated.SubFailureDomain = "dummy"
		err := validatePool(context, clusterInfo, clusterSpec, &p)
		assert.Error(t, err)
	})

	t.Run("succeed with ec pool and valid compression mode", func(t *testing.T) {
		p := cephv1.CephBlockPool{ObjectMeta: metav1.ObjectMeta{Name: "mypool", Namespace: clusterInfo.Namespace}}
		p.Spec.ErasureCoded.CodingChunks = 1
		p.Spec.ErasureCoded.DataChunks = 2
		p.Spec.CompressionMode = "passive"
		err := validatePool(context, clusterInfo, clusterSpec, &p)
		assert.NoError(t, err)
	})

	t.Run("fail unrecognized mirroring mode", func(t *testing.T) {
		p := cephv1.CephBlockPool{ObjectMeta: metav1.ObjectMeta{Name: "mypool", Namespace: clusterInfo.Namespace}}
		p.Spec.Replicated.Size = 3
		p.Spec.Mirroring.Enabled = true
		p.Spec.Mirroring.Mode = "foo"
		err := validatePool(context, clusterInfo, clusterSpec, &p)
		assert.Error(t, err)
		assert.EqualError(t, err, "unrecognized mirroring mode \"foo\". only 'image and 'pool' are supported")
	})

	t.Run("success known mirroring mode", func(t *testing.T) {
		p := cephv1.CephBlockPool{ObjectMeta: metav1.ObjectMeta{Name: "mypool", Namespace: clusterInfo.Namespace}}
		p.Spec.Replicated.Size = 3
		p.Spec.Mirroring.Enabled = true
		p.Spec.Mirroring.Mode = "pool"
		err := validatePool(context, clusterInfo, clusterSpec, &p)
		assert.NoError(t, err)
	})

	t.Run("fail mirroring mode no interval specified", func(t *testing.T) {
		p := cephv1.CephBlockPool{ObjectMeta: metav1.ObjectMeta{Name: "mypool", Namespace: clusterInfo.Namespace}}
		p.Spec.Replicated.Size = 3
		p.Spec.Mirroring.Enabled = true
		p.Spec.Mirroring.Mode = "pool"
		p.Spec.Mirroring.SnapshotSchedules = []cephv1.SnapshotScheduleSpec{{StartTime: "14:00:00-05:00"}}
		err := validatePool(context, clusterInfo, clusterSpec, &p)
		assert.Error(t, err)
		assert.EqualError(t, err, "schedule interval cannot be empty if start time is specified")
	})

	t.Run("fail mirroring mode we have a snap interval", func(t *testing.T) {
		p := cephv1.CephBlockPool{ObjectMeta: metav1.ObjectMeta{Name: "mypool", Namespace: clusterInfo.Namespace}}
		p.Spec.Replicated.Size = 3
		p.Spec.Mirroring.Enabled = true
		p.Spec.Mirroring.Mode = "pool"
		p.Spec.Mirroring.SnapshotSchedules = []cephv1.SnapshotScheduleSpec{{Interval: "24h"}}
		err := validatePool(context, clusterInfo, clusterSpec, &p)
		assert.NoError(t, err)
	})

	t.Run("failure and subfailure domains", func(t *testing.T) {
		p := cephv1.CephBlockPool{ObjectMeta: metav1.ObjectMeta{Name: "mypool", Namespace: clusterInfo.Namespace}}
		p.Spec.Replicated.Size = 3
		p.Spec.FailureDomain = "host"
		p.Spec.Replicated.SubFailureDomain = "host"
		err := validatePool(context, clusterInfo, clusterSpec, &p)
		assert.Error(t, err)
		assert.EqualError(t, err, "failure and subfailure domain cannot be identical")
	})
}

func TestValidateCrushProperties(t *testing.T) {
	executor := &exectest.MockExecutor{}
	context := &clusterd.Context{Executor: executor}
	clusterInfo := cephclient.AdminTestClusterInfo("mycluster")
	executor.MockExecuteCommandWithOutput = func(command string, args ...string) (string, error) {
		logger.Infof("Command: %s %v", command, args)
		if args[1] == "crush" && args[2] == "dump" {
			return `{"types":[{"type_id": 0,"name": "osd"}],"buckets":[{"id": -1,"name":"default"},{"id": -2,"name":"good"}, {"id": -3,"name":"host"}]}`, nil
		}
		return "", errors.Errorf("unexpected ceph command %q", args)
	}

	// succeed with a failure domain that exists
	p := &cephv1.CephBlockPool{
		ObjectMeta: metav1.ObjectMeta{Name: "mypool", Namespace: clusterInfo.Namespace},
		Spec: cephv1.NamedBlockPoolSpec{
			PoolSpec: cephv1.PoolSpec{
				Replicated: cephv1.ReplicatedSpec{Size: 1, RequireSafeReplicaSize: false},
			},
		},
	}
	clusterSpec := &cephv1.ClusterSpec{}

	err := validatePool(context, clusterInfo, clusterSpec, p)
	assert.Nil(t, err)

	// fail with a failure domain that doesn't exist
	p.Spec.FailureDomain = "doesntexist"
	err = validatePool(context, clusterInfo, clusterSpec, p)
	assert.NotNil(t, err)

	// fail with a crush root that doesn't exist
	p.Spec.FailureDomain = "osd"
	p.Spec.CrushRoot = "bad"
	err = validatePool(context, clusterInfo, clusterSpec, p)
	assert.NotNil(t, err)

	// fail with a crush root that does exist
	p.Spec.CrushRoot = "good"
	err = validatePool(context, clusterInfo, clusterSpec, p)
	assert.Nil(t, err)

	// Success replica size is 4 and replicasPerFailureDomain is 2
	p.Spec.Replicated.Size = 4
	p.Spec.Replicated.ReplicasPerFailureDomain = 2
	err = validatePool(context, clusterInfo, clusterSpec, p)
	assert.NoError(t, err)
}

// writeAffinityCrushMapJSON is a crush map with a default root holding zones a, b, c, us-east, and
// a separate alt root holding zone x. Zones a and b only have ssd OSDs while zone c only has hdd OSDs.
const writeAffinityCrushMapJSON = `{
  "devices": [
    {"id": 0, "name": "osd.0", "class": "ssd"},
    {"id": 1, "name": "osd.1", "class": "ssd"},
    {"id": 2, "name": "osd.2", "class": "ssd"},
    {"id": 3, "name": "osd.3", "class": "ssd"},
    {"id": 4, "name": "osd.4", "class": "hdd"},
    {"id": 5, "name": "osd.5", "class": "hdd"},
    {"id": 6, "name": "osd.6", "class": "ssd"},
    {"id": 7, "name": "osd.7", "class": "hdd"},
    {"id": 8, "name": "osd.8", "class": "ssd"},
    {"id": 9, "name": "osd.9", "class": "hdd"}
  ],
  "types": [
    {"type_id": 0, "name": "osd"},
    {"type_id": 1, "name": "host"},
    {"type_id": 9, "name": "zone"},
    {"type_id": 10, "name": "root"}
  ],
  "buckets": [
    {"id": -1, "name": "default", "type_id": 10, "type_name": "root", "items": [{"id": -10, "weight": 1, "pos": 0}, {"id": -11, "weight": 1, "pos": 1}, {"id": -12, "weight": 1, "pos": 2}, {"id": -13, "weight": 1, "pos": 3}, {"id": -14, "weight": 1, "pos": 4}]},
    {"id": -2, "name": "alt", "type_id": 10, "type_name": "root", "items": [{"id": -30, "weight": 1, "pos": 0}]},
    {"id": -10, "name": "a", "type_id": 9, "type_name": "zone", "items": [{"id": -20, "weight": 1, "pos": 0}, {"id": -21, "weight": 1, "pos": 1}]},
    {"id": -11, "name": "b", "type_id": 9, "type_name": "zone", "items": [{"id": -22, "weight": 1, "pos": 0}, {"id": -23, "weight": 1, "pos": 1}]},
    {"id": -12, "name": "c", "type_id": 9, "type_name": "zone", "items": [{"id": -24, "weight": 1, "pos": 0}, {"id": -25, "weight": 1, "pos": 1}]},
    {"id": -13, "name": "us-east", "type_id": 9, "type_name": "zone", "items": [{"id": -27, "weight": 1, "pos": 0}]},
    {"id": -14, "name": "d", "type_id": 9, "type_name": "zone", "items": [{"id": -28, "weight": 1, "pos": 0}, {"id": -29, "weight": 1, "pos": 1}]},
    {"id": -30, "name": "x", "type_id": 9, "type_name": "zone", "items": [{"id": -26, "weight": 1, "pos": 0}]},
    {"id": -20, "name": "a1", "type_id": 1, "type_name": "host", "items": [{"id": 0, "weight": 1, "pos": 0}]},
    {"id": -21, "name": "a2", "type_id": 1, "type_name": "host", "items": [{"id": 1, "weight": 1, "pos": 0}]},
    {"id": -22, "name": "b1", "type_id": 1, "type_name": "host", "items": [{"id": 2, "weight": 1, "pos": 0}]},
    {"id": -23, "name": "b2", "type_id": 1, "type_name": "host", "items": [{"id": 3, "weight": 1, "pos": 0}]},
    {"id": -24, "name": "c1", "type_id": 1, "type_name": "host", "items": [{"id": 4, "weight": 1, "pos": 0}]},
    {"id": -25, "name": "c2", "type_id": 1, "type_name": "host", "items": [{"id": 5, "weight": 1, "pos": 0}]},
    {"id": -27, "name": "us-east1", "type_id": 1, "type_name": "host", "items": [{"id": 6, "weight": 1, "pos": 0}]},
    {"id": -28, "name": "d1", "type_id": 1, "type_name": "host", "items": [{"id": 8, "weight": 1, "pos": 0}]},
    {"id": -29, "name": "d2", "type_id": 1, "type_name": "host", "items": [{"id": 9, "weight": 1, "pos": 0}]},
    {"id": -100, "name": "d1~ssd", "type_id": 1, "type_name": "host", "items": [{"id": 8, "weight": 1, "pos": 0}]},
    {"id": -101, "name": "a1~ssd", "type_id": 1, "type_name": "host", "items": [{"id": 0, "weight": 1, "pos": 0}]},
    {"id": -26, "name": "x1", "type_id": 1, "type_name": "host", "items": [{"id": 7, "weight": 1, "pos": 0}]}
  ]
}`

func writeAffinityCrushMap(t *testing.T) cephclient.CrushMap {
	t.Helper()
	var crush cephclient.CrushMap
	assert.NoError(t, json.Unmarshal([]byte(writeAffinityCrushMapJSON), &crush))
	return crush
}

func writeAffinityPoolSpec(failureDomain string, size, replicasPerFailureDomain uint, failureDomains ...string) cephv1.PoolSpec {
	return cephv1.PoolSpec{
		FailureDomain: failureDomain,
		Replicated: cephv1.ReplicatedSpec{
			Size:                     size,
			ReplicasPerFailureDomain: replicasPerFailureDomain,
			WriteAffinity:            &cephv1.WriteAffinitySpec{FailureDomains: failureDomains},
		},
	}
}

func TestValidateWriteAffinity(t *testing.T) {
	crush := writeAffinityCrushMap(t)
	clusterSpec := &cephv1.ClusterSpec{}

	deviceClassSpec := writeAffinityPoolSpec("zone", 3, 0, "a", "b", "c")
	deviceClassSpec.DeviceClass = "ssd"

	rootSubFailureDomainSpec := writeAffinityPoolSpec("zone", 3, 0, "a", "b", "c")
	rootSubFailureDomainSpec.Replicated.SubFailureDomain = "root"

	deviceClassHostsSpec := writeAffinityPoolSpec("zone", 6, 2, "a", "b", "d")
	deviceClassHostsSpec.DeviceClass = "ssd"

	sizeParameterSpec := writeAffinityPoolSpec("zone", 3, 0, "a", "b", "c")
	sizeParameterSpec.Parameters = map[string]string{"size": "4"}

	crushRuleParameterSpec := writeAffinityPoolSpec("zone", 3, 0, "a", "b", "c")
	crushRuleParameterSpec.Parameters = map[string]string{"crush_rule": "replicated_rule"}

	tests := []struct {
		name        string
		spec        cephv1.PoolSpec
		expectedErr string
	}{
		{
			name: "valid spec",
			spec: writeAffinityPoolSpec("zone", 3, 0, "a", "b", "c"),
		},
		{
			name:        "missing failureDomain",
			spec:        writeAffinityPoolSpec("", 3, 0, "a", "b", "c"),
			expectedErr: "failureDomain must be set",
		},
		{
			name: "non replicated pool",
			spec: cephv1.PoolSpec{
				Replicated: cephv1.ReplicatedSpec{
					WriteAffinity: &cephv1.WriteAffinitySpec{FailureDomains: []string{"a", "b"}},
				},
			},
			expectedErr: "writeAffinity requires a replicated pool",
		},
		{
			name:        "size mismatch",
			spec:        writeAffinityPoolSpec("zone", 4, 0, "a", "b", "c"),
			expectedErr: "size 4 must equal the number of failureDomains (3) multiplied by replicasPerFailureDomain (1)",
		},
		{
			name:        "failureDomain equals default subFailureDomain",
			spec:        writeAffinityPoolSpec("host", 3, 0, "a1", "a2", "b1"),
			expectedErr: `failureDomain and subFailureDomain "host" cannot be identical`,
		},
		{
			name:        "bucket not found",
			spec:        writeAffinityPoolSpec("zone", 3, 0, "a", "b", "nope"),
			expectedErr: `crush bucket "nope" of type "zone" not found`,
		},
		{
			name:        "bucket of wrong type",
			spec:        writeAffinityPoolSpec("zone", 3, 0, "a", "b", "a1"),
			expectedErr: `crush bucket "a1" of type "zone" not found`,
		},
		{
			name:        "bucket not under crush root",
			spec:        writeAffinityPoolSpec("zone", 3, 0, "a", "b", "x"),
			expectedErr: `crush bucket "x" is not under the crush root "default"`,
		},
		{
			name:        "device class missing from a failure domain",
			spec:        deviceClassSpec,
			expectedErr: `crush bucket "c" has no OSDs of device class "ssd"`,
		},
		{
			name:        "subFailureDomain not below the failure domains",
			spec:        rootSubFailureDomainSpec,
			expectedErr: `crush bucket "a" has 0 buckets of type "root" with OSDs, at least 1 are required`,
		},
		{
			name:        "not enough subFailureDomain buckets for replicasPerFailureDomain",
			spec:        writeAffinityPoolSpec("zone", 24, 8, "a", "b", "c"),
			expectedErr: `buckets of type "host" with OSDs, at least 8 are required`,
		},
		{
			name: "two hosts per failure domain",
			spec: writeAffinityPoolSpec("zone", 6, 2, "a", "b", "d"),
		},
		{
			name:        "hosts without OSDs of the device class are not counted",
			spec:        deviceClassHostsSpec,
			expectedErr: `crush bucket "d" has 1 buckets of type "host" with OSDs of device class "ssd", at least 2 are required`,
		},
		{
			name:        "size parameter rejected",
			spec:        sizeParameterSpec,
			expectedErr: `parameter "size" cannot be set`,
		},
		{
			name:        "crush_rule parameter rejected",
			spec:        crushRuleParameterSpec,
			expectedErr: `parameter "crush_rule" cannot be set`,
		},
		{
			name: "failure domain name is normalized",
			spec: writeAffinityPoolSpec("zone", 3, 0, "us.east", "b", "a"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateWriteAffinity(clusterSpec, &tt.spec, crush)
			if tt.expectedErr == "" {
				assert.NoError(t, err)
				return
			}
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectedErr)
		})
	}
}

func TestCountCrushBucketsSharedHost(t *testing.T) {
	// host h1 is linked under rack r1 of zone a and under rack r2 of zone b
	buckets := []string{
		`{"id": -1, "name": "a", "type_name": "zone", "items": [{"id": -10}, {"id": -11}]}`,
		`{"id": -2, "name": "b", "type_name": "zone", "items": [{"id": -12}]}`,
		`{"id": -10, "name": "r1", "type_name": "rack", "items": [{"id": -20}]}`,
		`{"id": -11, "name": "r3", "type_name": "rack", "items": [{"id": -21}]}`,
		`{"id": -12, "name": "r2", "type_name": "rack", "items": [{"id": -20}]}`,
		`{"id": -20, "name": "h1", "type_name": "host", "items": [{"id": 0}]}`,
		`{"id": -21, "name": "h2", "type_name": "host", "items": [{"id": 1}]}`,
	}
	reversed := slices.Clone(buckets)
	slices.Reverse(reversed)

	for _, order := range [][]string{buckets, reversed} {
		var crush cephclient.CrushMap
		crushJSON := `{"devices": [{"id": 0, "class": "ssd"}, {"id": 1, "class": "ssd"}], "buckets": [` + strings.Join(order, ",") + `]}`
		assert.NoError(t, json.Unmarshal([]byte(crushJSON), &crush))
		parents := crushParents(crush)
		assert.Equal(t, 2, countCrushBuckets(crush, parents, crushSubtree(crush, "a"), "rack", "ssd"))
		assert.Equal(t, 1, countCrushBuckets(crush, parents, crushSubtree(crush, "b"), "rack", "ssd"))
		assert.Equal(t, 2, countCrushBuckets(crush, parents, crushSubtree(crush, "a"), "host", ""))
		assert.Equal(t, 2, countCrushBuckets(crush, parents, crushSubtree(crush, "a"), "osd", "ssd"))
	}
}

func TestValidateWriteAffinitySharedOSD(t *testing.T) {
	// host h1 is linked under zone a and zone b
	crushJSON := `{"devices": [{"id": 0}, {"id": 1}, {"id": 2}], "buckets": [
		{"id": -1, "name": "default", "type_name": "root", "items": [{"id": -2}, {"id": -3}]},
		{"id": -2, "name": "a", "type_name": "zone", "items": [{"id": -10}, {"id": -11}]},
		{"id": -3, "name": "b", "type_name": "zone", "items": [{"id": -10}, {"id": -12}]},
		{"id": -10, "name": "h1", "type_name": "host", "items": [{"id": 0}]},
		{"id": -11, "name": "h2", "type_name": "host", "items": [{"id": 1}]},
		{"id": -12, "name": "h3", "type_name": "host", "items": [{"id": 2}]}]}`
	var crush cephclient.CrushMap
	assert.NoError(t, json.Unmarshal([]byte(crushJSON), &crush))
	spec := writeAffinityPoolSpec("zone", 2, 0, "a", "b")
	err := validateWriteAffinity(&cephv1.ClusterSpec{}, &spec, crush)
	assert.ErrorContains(t, err, `crush buckets "a" and "b" share OSD 0`)
}

func TestValidateWriteAffinityWithCrushDump(t *testing.T) {
	executor := &exectest.MockExecutor{}
	executor.MockExecuteCommandWithOutput = func(command string, args ...string) (string, error) {
		if args[0] == "osd" && args[1] == "crush" && args[2] == "dump" {
			return writeAffinityCrushMapJSON, nil
		}
		return "", errors.Errorf("unexpected ceph command %q", args)
	}
	context := &clusterd.Context{Executor: executor}
	clusterInfo := cephclient.AdminTestClusterInfo("mycluster")
	clusterSpec := &cephv1.ClusterSpec{}

	newPool := func(failureDomains ...string) *cephv1.CephBlockPool {
		return &cephv1.CephBlockPool{
			ObjectMeta: metav1.ObjectMeta{Name: "mypool", Namespace: clusterInfo.Namespace},
			Spec: cephv1.NamedBlockPoolSpec{
				PoolSpec: writeAffinityPoolSpec("zone", 3, 0, failureDomains...),
			},
		}
	}

	t.Run("valid spec", func(t *testing.T) {
		assert.NoError(t, validatePool(context, clusterInfo, clusterSpec, newPool("a", "b", "c")))
	})

	t.Run("bucket not under crush root", func(t *testing.T) {
		err := validatePool(context, clusterInfo, clusterSpec, newPool("a", "b", "x"))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), `crush bucket "x" is not under the crush root "default"`)
	})
}

func TestValidateWriteAffinityStretchCluster(t *testing.T) {
	crush := writeAffinityCrushMap(t)

	newStretchClusterSpec := func(failureDomainLabel string) *cephv1.ClusterSpec {
		return &cephv1.ClusterSpec{
			Mon: cephv1.MonSpec{
				StretchCluster: &cephv1.StretchClusterSpec{
					FailureDomainLabel: failureDomainLabel,
					Zones: []cephv1.MonZoneSpec{
						{Name: "a"},
						{Name: "b"},
						{Name: "c", Arbiter: true},
					},
				},
			},
		}
	}

	t.Run("valid data zones", func(t *testing.T) {
		spec := writeAffinityPoolSpec("zone", 4, 2, "a", "b")
		assert.NoError(t, validateWriteAffinity(newStretchClusterSpec(""), &spec, crush))
	})

	t.Run("device class rejected", func(t *testing.T) {
		spec := writeAffinityPoolSpec("zone", 4, 2, "a", "b")
		spec.DeviceClass = "ssd"
		err := validateWriteAffinity(newStretchClusterSpec(""), &spec, crush)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "deviceClass is not supported in a stretch cluster")
	})

	t.Run("arbiter zone rejected", func(t *testing.T) {
		spec := writeAffinityPoolSpec("zone", 4, 2, "a", "c")
		err := validateWriteAffinity(newStretchClusterSpec(""), &spec, crush)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failureDomains must be the non-arbiter stretch cluster zones [a b]")
	})

	t.Run("failureDomain other than zone rejected", func(t *testing.T) {
		spec := writeAffinityPoolSpec("host", 4, 2, "a", "b")
		spec.Replicated.SubFailureDomain = "osd"
		err := validateWriteAffinity(newStretchClusterSpec(""), &spec, crush)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), `failureDomain must be the stretch cluster failure domain "zone"`)
	})

	t.Run("region failure domain label requires region failureDomain", func(t *testing.T) {
		spec := writeAffinityPoolSpec("zone", 4, 2, "a", "b")
		err := validateWriteAffinity(newStretchClusterSpec("topology.kubernetes.io/region"), &spec, crush)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), `failureDomain must be the stretch cluster failure domain "region"`)
	})

	t.Run("region failure domain label accepted with region failureDomain", func(t *testing.T) {
		spec := writeAffinityPoolSpec("region", 4, 2, "a", "b")
		assert.NoError(t, validateStretchWriteAffinity(newStretchClusterSpec("topology.kubernetes.io/region"), &spec))
	})

	t.Run("mon failure domain label is used without a stretch cluster label", func(t *testing.T) {
		clusterSpec := newStretchClusterSpec("")
		clusterSpec.Mon.FailureDomainLabel = "topology.kubernetes.io/region"
		spec := writeAffinityPoolSpec("region", 4, 2, "a", "b")
		assert.NoError(t, validateStretchWriteAffinity(clusterSpec, &spec))
		spec.FailureDomain = "zone"
		assert.ErrorContains(t, validateStretchWriteAffinity(clusterSpec, &spec), `failureDomain must be the stretch cluster failure domain "region"`)
	})
}

func TestValidatePoolSpecRejectsWriteAffinity(t *testing.T) {
	context := &clusterd.Context{}
	clusterInfo := &cephclient.ClusterInfo{Namespace: "myns"}
	clusterSpec := &cephv1.ClusterSpec{}

	p := writeAffinityPoolSpec("zone", 2, 0, "a", "b")
	err := ValidatePoolSpec(context, clusterInfo, clusterSpec, &p)
	assert.EqualError(t, err, "writeAffinity is only supported for CephBlockPool")

	p = cephv1.PoolSpec{Replicated: cephv1.ReplicatedSpec{Size: 3}}
	assert.NoError(t, ValidatePoolSpec(context, clusterInfo, clusterSpec, &p))
}

func TestValidateDeviceClasses(t *testing.T) {
	testcases := []struct {
		name                       string
		primaryDeviceClassOutput   string
		secondaryDeviceClassOutput string
		hybridStorageSpec          *cephv1.HybridStorageSpec
		isValidSpec                bool
	}{
		{
			name:                       "valid hybridStorageSpec",
			primaryDeviceClassOutput:   "[0, 1, 2]",
			secondaryDeviceClassOutput: "[3, 4, 5]",
			hybridStorageSpec: &cephv1.HybridStorageSpec{
				PrimaryDeviceClass:   "ssd",
				SecondaryDeviceClass: "hdd",
			},
			isValidSpec: true,
		},
		{
			name:                       "invalid hybridStorageSpec.PrimaryDeviceClass",
			primaryDeviceClassOutput:   "[]",
			secondaryDeviceClassOutput: "[3, 4, 5]",
			hybridStorageSpec: &cephv1.HybridStorageSpec{
				PrimaryDeviceClass:   "ssd",
				SecondaryDeviceClass: "hdd",
			},
			isValidSpec: false,
		},
		{
			name:                       "invalid hybridStorageSpec.SecondaryDeviceClass",
			primaryDeviceClassOutput:   "[0, 1, 2]",
			secondaryDeviceClassOutput: "[]",
			hybridStorageSpec: &cephv1.HybridStorageSpec{
				PrimaryDeviceClass:   "ssd",
				SecondaryDeviceClass: "hdd",
			},
			isValidSpec: false,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			clusterInfo := cephclient.AdminTestClusterInfo("mycluster")
			executor := &exectest.MockExecutor{}
			context := &clusterd.Context{Executor: executor}
			executor.MockExecuteCommandWithOutput = func(command string, args ...string) (string, error) {
				logger.Infof("ExecuteCommandWithOutputFile: %s %v", command, args)
				if args[1] == "crush" && args[2] == "class" && args[3] == "ls-osd" && args[4] == "ssd" {
					// Mock executor for `ceph osd crush class ls-osd ssd`
					return tc.primaryDeviceClassOutput, nil
				} else if args[1] == "crush" && args[2] == "class" && args[3] == "ls-osd" && args[4] == "hdd" {
					// Mock executor for `ceph osd crush class ls-osd hdd`
					return tc.secondaryDeviceClassOutput, nil
				}
				return "", nil
			}

			p := &cephv1.PoolSpec{
				Replicated: cephv1.ReplicatedSpec{
					HybridStorage: tc.hybridStorageSpec,
				},
			}

			err := validateDeviceClasses(context, clusterInfo, p)
			if tc.isValidSpec {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
