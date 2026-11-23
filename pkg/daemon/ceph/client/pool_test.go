/*
Copyright 2016 The Rook Authors. All rights reserved.

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
package client

import (
	"fmt"
	"os/exec"
	"reflect"
	"slices"
	"strconv"
	"testing"

	"github.com/pkg/errors"
	cephv1 "github.com/rook/rook/pkg/apis/ceph.rook.io/v1"
	"github.com/rook/rook/pkg/clusterd"
	exectest "github.com/rook/rook/pkg/util/exec/test"
	"github.com/stretchr/testify/assert"
)

const emptyApplicationName = `{"":{}}`

func TestCreateECPoolWithOverwrites(t *testing.T) {
	testCreateECPool(t, true, "")
}

func TestCreateECPoolWithoutOverwrites(t *testing.T) {
	testCreateECPool(t, false, "")
}

func TestCreateECPoolWithCompression(t *testing.T) {
	testCreateECPool(t, false, "aggressive")
	testCreateECPool(t, true, "none")
}

func testCreateECPool(t *testing.T, overwrite bool, compressionMode string) {
	compressionModeCreated := false
	p := cephv1.NamedPoolSpec{
		Name: "mypool",
		PoolSpec: cephv1.PoolSpec{
			FailureDomain: "host",
			ErasureCoded:  cephv1.ErasureCodedSpec{},
		},
	}
	if compressionMode != "" {
		p.CompressionMode = compressionMode
	}
	executor := &exectest.MockExecutor{}
	context := &clusterd.Context{Executor: executor}
	executor.MockExecuteCommandWithOutput = func(command string, args ...string) (string, error) {
		logger.Infof("Command: %s %v", command, args)
		if args[1] == "pool" {
			if args[2] == "create" {
				assert.Equal(t, "mypool", args[3])
				assert.Equal(t, "erasure", args[5])
				assert.Equal(t, "mypoolprofile", args[6])
				return "", nil
			}
			if args[2] == "set" {
				assert.Equal(t, "mypool", args[3])
				if args[4] == "allow_ec_overwrites" {
					assert.Equal(t, true, overwrite)
					assert.Equal(t, "true", args[5])
					return "", nil
				}
				if args[4] == "compression_mode" {
					assert.Equal(t, compressionMode, args[5])
					compressionModeCreated = true
					return "", nil
				}
			}
			if args[2] == "application" {
				if args[3] == "get" {
					return emptyApplicationName, nil
				}
				assert.Equal(t, "enable", args[3])
				assert.Equal(t, "mypool", args[4])
				assert.Equal(t, "myapp", args[5])
				return "", nil
			}
		}
		return "", errors.Errorf("unexpected ceph command %q", args)
	}

	err := createECPoolForApp(context, AdminTestClusterInfo("mycluster"), "mypoolprofile", p, DefaultPGCount, overwrite)
	assert.Nil(t, err)
	if compressionMode != "" {
		assert.True(t, compressionModeCreated)
	} else {
		assert.False(t, compressionModeCreated)
	}
}

func TestSetPoolApplication(t *testing.T) {
	poolName := "testpool"
	appName := "testapp"
	setAppName := false
	blankAppName := false
	clusterInfo := AdminTestClusterInfo("mycluster")
	executor := &exectest.MockExecutor{}
	context := &clusterd.Context{Executor: executor}
	executor.MockExecuteCommandWithOutput = func(command string, args ...string) (string, error) {
		logger.Infof("Command: %s %v", command, args)
		if args[1] == "pool" && args[2] == "application" {
			if args[3] == "get" {
				assert.Equal(t, poolName, args[4])
				if blankAppName {
					return emptyApplicationName, nil
				} else {
					return fmt.Sprintf(`{"%s":{}}`, appName), nil
				}
			}
			if args[3] == "enable" {
				setAppName = true
				assert.Equal(t, poolName, args[4])
				assert.Equal(t, appName, args[5])
				return "", nil
			}
		}
		return "", errors.Errorf("unexpected ceph command %q", args)
	}

	t.Run("set pool application", func(t *testing.T) {
		setAppName = false
		blankAppName = true
		err := givePoolAppTag(context, clusterInfo, poolName, appName)
		assert.NoError(t, err)
		assert.True(t, setAppName)
	})

	t.Run("pool application already set", func(t *testing.T) {
		setAppName = false
		blankAppName = false
		err := givePoolAppTag(context, clusterInfo, poolName, appName)
		assert.NoError(t, err)
		assert.False(t, setAppName)
	})
}

func TestCreateReplicaPoolWithFailureDomain(t *testing.T) {
	testCreateReplicaPool(t, "osd", "mycrushroot", "", "")
}

func TestCreateReplicaPoolWithDeviceClass(t *testing.T) {
	testCreateReplicaPool(t, "osd", "mycrushroot", "hdd", "")
}

func TestCreateReplicaPoolWithCompression(t *testing.T) {
	testCreateReplicaPool(t, "osd", "mycrushroot", "hdd", "passive")
	testCreateReplicaPool(t, "osd", "mycrushroot", "hdd", "force")
}

func testCreateReplicaPool(t *testing.T, failureDomain, crushRoot, deviceClass, compressionMode string) {
	crushRuleCreated := false
	compressionModeCreated := false
	executor := &exectest.MockExecutor{}
	context := &clusterd.Context{Executor: executor}
	executor.MockExecuteCommandWithOutput = func(command string, args ...string) (string, error) {
		logger.Infof("Command: %s %v", command, args)
		if args[1] == "pool" {
			if args[2] == "create" {
				assert.Equal(t, "mypool", args[3])
				assert.Equal(t, "replicated", args[5])
				assert.Equal(t, "--size", args[7])
				assert.Equal(t, "12345", args[8])
				return "", nil
			}
			if args[2] == "get" {
				assert.Equal(t, "mypool", args[3])
				assert.Equal(t, "all", args[4])
				return `{"pool":"replicapool","pool_id":2,"size":1,"min_size":1,"crush_rule":"replicapool_osd"}`, nil
			}
			if args[2] == "set" {
				assert.Equal(t, "mypool", args[3])
				if args[4] == "size" {
					assert.Equal(t, "12345", args[5])
				}
				if args[4] == "compression_mode" {
					assert.Equal(t, compressionMode, args[5])
					compressionModeCreated = true
				}
				return "", nil
			}
			if args[2] == "application" {
				if args[3] == "get" {
					return emptyApplicationName, nil
				}
				assert.Equal(t, "enable", args[3])
				assert.Equal(t, "mypool", args[4])
				assert.Equal(t, "myapp", args[5])
				return "", nil
			}
		}
		if args[1] == "lspools" {
			return `[{"poolnum":2,"poolname":"mypool"}]`, nil
		}
		if args[1] == "crush" {
			if args[2] == "dump" {
				return `{"rules":[{"rule_name":"replicapool_osd"}]}`, nil
			}
			crushRuleCreated = true
			assert.Equal(t, "rule", args[2])
			if args[3] == "dump" {
				assert.Equal(t, "replicapool_osd", args[4])
				return `{"rule_id": 3,"rule_name": "replicapool_osd","type": 1}`, nil
			}
			assert.Equal(t, "create-replicated", args[3])
			assert.Contains(t, args[4], "mypool")
			if crushRoot == "" {
				assert.Equal(t, "cluster-crush-root", args[5])
			} else {
				assert.Equal(t, crushRoot, args[5])
			}
			if failureDomain == "" {
				assert.Equal(t, "host", args[6])
			} else {
				assert.Equal(t, failureDomain, args[6])
			}
			if deviceClass == "" {
				assert.False(t, slices.Contains(args, "hdd"))
			} else {
				assert.Equal(t, deviceClass, args[7])
			}
			return "", nil
		}
		return "", errors.Errorf("unexpected ceph command %q", args)
	}

	p := cephv1.NamedPoolSpec{
		Name: "mypool",
		PoolSpec: cephv1.PoolSpec{
			FailureDomain: failureDomain, CrushRoot: crushRoot, DeviceClass: deviceClass,
			Replicated: cephv1.ReplicatedSpec{Size: 12345},
		},
	}
	if compressionMode != "" {
		p.CompressionMode = compressionMode
	}
	clusterSpec := &cephv1.ClusterSpec{Storage: cephv1.StorageScopeSpec{Config: map[string]string{CrushRootConfigKey: "cluster-crush-root"}}}
	err := createReplicatedPoolForApp(context, AdminTestClusterInfo("mycluster"), clusterSpec, p, DefaultPGCount)
	assert.Nil(t, err)
	// The base crush rule should not be recreated for an existing pool.
	assert.False(t, crushRuleCreated)
	if compressionMode != "" {
		assert.True(t, compressionModeCreated)
	} else {
		assert.False(t, compressionModeCreated)
	}
}

func TestUpdateFailureDomain(t *testing.T) {
	var newCrushRule string
	currentFailureDomain := "rack"
	currentDeviceClass := "default"
	testCrushRuleName := "test_rule"
	cephCommandCalled := false
	executor := &exectest.MockExecutor{}
	context := &clusterd.Context{Executor: executor}
	executor.MockExecuteCommandWithOutput = func(command string, args ...string) (string, error) {
		logger.Infof("Command: %s %v", command, args)
		cephCommandCalled = true
		if args[1] == "pool" {
			if args[2] == "get" {
				assert.Equal(t, "mypool", args[3])
				return fmt.Sprintf(`{"crush_rule": "%s"}`, testCrushRuleName), nil
			}
			if args[2] == "set" {
				assert.Equal(t, "mypool", args[3])
				assert.Equal(t, "crush_rule", args[4])
				newCrushRule = args[5]
				return "", nil
			}
		}
		if args[1] == "crush" {
			if args[2] == "rule" && args[3] == "dump" {
				return fmt.Sprintf(`{"steps": [{"item_name":"%s"},{"type":"%s"}]}`, currentDeviceClass, currentFailureDomain), nil
			}
			newCrushRule = "foo"
			return "", nil
		}
		return "", errors.Errorf("unexpected ceph command %q", args)
	}

	enableCrushUpdates := true
	t.Run("no desired failure domain", func(t *testing.T) {
		p := cephv1.NamedPoolSpec{
			Name: "mypool",
			PoolSpec: cephv1.PoolSpec{
				Replicated:         cephv1.ReplicatedSpec{Size: 3},
				EnableCrushUpdates: &enableCrushUpdates,
			},
		}
		clusterSpec := &cephv1.ClusterSpec{Storage: cephv1.StorageScopeSpec{}}
		err := updatePoolCrushRule(context, AdminTestClusterInfo("mycluster"), clusterSpec, p)
		assert.NoError(t, err)
		assert.Equal(t, "", newCrushRule)
	})

	t.Run("same failure domain", func(t *testing.T) {
		p := cephv1.NamedPoolSpec{
			Name: "mypool",
			PoolSpec: cephv1.PoolSpec{
				FailureDomain:      currentFailureDomain,
				Replicated:         cephv1.ReplicatedSpec{Size: 3},
				EnableCrushUpdates: &enableCrushUpdates,
			},
		}
		testCrushRuleName = "mypool_rack"
		clusterSpec := &cephv1.ClusterSpec{Storage: cephv1.StorageScopeSpec{}}
		err := updatePoolCrushRule(context, AdminTestClusterInfo("mycluster"), clusterSpec, p)
		assert.NoError(t, err)
		assert.Equal(t, "", newCrushRule)
	})

	t.Run("trying to change failure domain without enabling EnableCrushUpdates", func(t *testing.T) {
		p := cephv1.NamedPoolSpec{
			Name: "mypool",
			PoolSpec: cephv1.PoolSpec{
				FailureDomain: "zone",
				Replicated:    cephv1.ReplicatedSpec{Size: 3},
			},
		}
		clusterSpec := &cephv1.ClusterSpec{Storage: cephv1.StorageScopeSpec{}}
		err := updatePoolCrushRule(context, AdminTestClusterInfo("mycluster"), clusterSpec, p)
		assert.NoError(t, err)
		assert.Equal(t, "", newCrushRule)
	})

	t.Run("changing failure domain", func(t *testing.T) {
		p := cephv1.NamedPoolSpec{
			Name: "mypool",
			PoolSpec: cephv1.PoolSpec{
				FailureDomain:      "zone",
				Replicated:         cephv1.ReplicatedSpec{Size: 3},
				EnableCrushUpdates: &enableCrushUpdates,
			},
		}
		clusterSpec := &cephv1.ClusterSpec{Storage: cephv1.StorageScopeSpec{}}
		err := updatePoolCrushRule(context, AdminTestClusterInfo("mycluster"), clusterSpec, p)
		assert.NoError(t, err)
		assert.Equal(t, "mypool_zone", newCrushRule)
	})

	t.Run("stretch cluster skips crush rule update", func(t *testing.T) {
		p := cephv1.NamedPoolSpec{
			Name: "mypool",
			PoolSpec: cephv1.PoolSpec{
				FailureDomain:      "zone",
				Replicated:         cephv1.ReplicatedSpec{Size: 3},
				EnableCrushUpdates: &enableCrushUpdates,
			},
		}
		clusterSpec := &cephv1.ClusterSpec{
			Mon:     cephv1.MonSpec{StretchCluster: &cephv1.StretchClusterSpec{Zones: []cephv1.MonZoneSpec{{Name: "zone1"}, {Name: "zone2"}, {Name: "zone3", Arbiter: true}}}},
			Storage: cephv1.StorageScopeSpec{},
		}
		newCrushRule = ""
		cephCommandCalled = false
		err := updatePoolCrushRule(context, AdminTestClusterInfo("mycluster"), clusterSpec, p)
		assert.NoError(t, err)
		assert.Equal(t, "", newCrushRule)
		assert.False(t, cephCommandCalled)
	})
}

func TestCleanupUnusedCrushRules(t *testing.T) {
	removedRules := []string{}
	executor := &exectest.MockExecutor{}
	context := &clusterd.Context{Executor: executor}
	executor.MockExecuteCommandWithOutput = func(command string, args ...string) (string, error) {
		logger.Infof("Command: %s %v", command, args)
		if command == "ceph" && args[1] == "lspools" {
			return `[{"poolnum":1,"poolname":"mypool"},{"poolnum":2,"poolname":"otherpool"}]`, nil
		}
		if command == "ceph" && args[1] == "pool" && args[2] == "get" {
			switch args[3] {
			case "mypool":
				return `{"pool":"mypool","pool_id":1,"crush_rule":"mypool_zone_ssd"}`, nil
			case "otherpool":
				return `{"pool":"otherpool","pool_id":2,"crush_rule":"shared_rule"}`, nil
			}
		}
		if command == "ceph" && args[1] == "crush" && args[2] == "dump" {
			return `{"rules":[
				{"rule_name":"mypool"},
				{"rule_name":"mypool_zone","steps":[{},{"type":"zone"}]},
				{"rule_name":"mypool_zone_ssd","steps":[{},{"type":"zone","item_name":"default~ssd"}]},
				{"rule_name":"shared_rule","steps":[{"item_name":"default"},{"type":"host"}]},
				{"rule_name":"custom_rule","steps":[{"item_name":"default"},{"type":"zone"}]},
				{"rule_name":"replicated_rule","steps":[{"item_name":"default"},{"type":"host"}]}
			]}`, nil
		}
		if command == "ceph" && args[1] == "crush" && args[2] == "rule" && args[3] == "rm" {
			removedRules = append(removedRules, args[4])
			return "", nil
		}

		return "", errors.Errorf("unexpected ceph command %q", args)
	}

	err := CleanupUnusedCrushRules(context, AdminTestClusterInfo("mycluster"))
	assert.NoError(t, err)
	slices.Sort(removedRules)
	expected := []string{"custom_rule", "mypool", "mypool_zone"}
	assert.Equal(t, expected, removedRules)
	assert.NotContains(t, removedRules, "replicated_rule")
}

func TestCleanupUnusedCrushRulesNoPools(t *testing.T) {
	removedRules := []string{}
	executor := &exectest.MockExecutor{}
	context := &clusterd.Context{Executor: executor}
	executor.MockExecuteCommandWithOutput = func(command string, args ...string) (string, error) {
		logger.Infof("Command: %s %v", command, args)
		if command == "ceph" && args[1] == "lspools" {
			return `[]`, nil
		}
		if command == "ceph" && args[1] == "crush" && args[2] == "rule" && args[3] == "rm" {
			removedRules = append(removedRules, args[4])
			return "", nil
		}
		return "", errors.Errorf("unexpected ceph command %q", args)
	}

	err := CleanupUnusedCrushRules(context, AdminTestClusterInfo("mycluster"))
	assert.NoError(t, err)
	assert.Empty(t, removedRules)
}

func TestExistingPoolDoesNotRecreateBaseCrushRule(t *testing.T) {
	crushRuleCreated := false
	executor := &exectest.MockExecutor{}
	context := &clusterd.Context{Executor: executor}
	executor.MockExecuteCommandWithOutput = func(command string, args ...string) (string, error) {
		logger.Infof("Command: %s %v", command, args)
		if args[1] == "pool" && args[2] == "get" {
			// Pool already exists and uses a suffixed crush rule
			return `{"pool":"mypool","pool_id":1,"size":3,"crush_rule":"mypool_host_ssd"}`, nil
		}
		if args[1] == "crush" && args[2] == "rule" {
			if args[3] == "create-replicated" {
				crushRuleCreated = true
				return "", nil
			}
			if args[3] == "dump" {
				return `{"steps": [{"item_name":"default~ssd"},{"type":"host"}]}`, nil
			}
		}
		if args[1] == "pool" && args[2] == "set" {
			return "", nil
		}
		return "", errors.Errorf("unexpected ceph command %q", args)
	}

	enableCrushUpdates := true
	p := cephv1.NamedPoolSpec{
		Name: "mypool",
		PoolSpec: cephv1.PoolSpec{
			FailureDomain:      "host",
			DeviceClass:        "ssd",
			Replicated:         cephv1.ReplicatedSpec{Size: 3},
			EnableCrushUpdates: &enableCrushUpdates,
		},
	}
	clusterSpec := &cephv1.ClusterSpec{Storage: cephv1.StorageScopeSpec{}}
	err := createReplicatedPoolForApp(context, AdminTestClusterInfo("mycluster"), clusterSpec, p, DefaultPGCount)
	assert.NoError(t, err)
	assert.False(t, crushRuleCreated, "base crush rule should not be created when the pool already exists")
}

func TestExtractPoolDetails(t *testing.T) {
	t.Run("complex crush rule skipped", func(t *testing.T) {
		rule := ruleSpec{Steps: []stepSpec{
			{Type: ""},
			{Type: ""},
			{Type: "zone"},
		}}
		failureDomain, _ := extractPoolDetails(rule)
		assert.Equal(t, "", failureDomain)
	})
	t.Run("valid crush rule", func(t *testing.T) {
		rule := ruleSpec{Steps: []stepSpec{
			{Type: ""},
			{Type: "zone", ItemName: "ssd"},
		}}
		failureDomain, deviceClass := extractPoolDetails(rule)
		assert.Equal(t, "zone", failureDomain)
		assert.Equal(t, "ssd", deviceClass)
	})

	t.Run("valid crush rule with crushroot combined", func(t *testing.T) {
		rule := ruleSpec{Steps: []stepSpec{
			{Type: ""},
			{Type: "zone", ItemName: "default~ssd"},
		}}
		failureDomain, deviceClass := extractPoolDetails(rule)
		assert.Equal(t, "zone", failureDomain)
		assert.Equal(t, "ssd", deviceClass)
	})
}

func TestGetPoolStatistics(t *testing.T) {
	p := PoolStatistics{}
	p.Images.Count = 1
	p.Images.ProvisionedBytes = 1024
	p.Images.SnapCount = 1
	p.Trash.Count = 1
	p.Trash.ProvisionedBytes = 2048
	p.Trash.SnapCount = 0
	executor := &exectest.MockExecutor{}
	context := &clusterd.Context{Executor: executor}
	executor.MockExecuteCommandWithOutput = func(command string, args ...string) (string, error) {
		a := "{\"images\":{\"count\":1,\"provisioned_bytes\":1024,\"snap_count\":1},\"trash\":{\"count\":1,\"provisioned_bytes\":2048,\"snap_count\":0}}"
		logger.Infof("Command: %s %v", command, args)

		if args[0] == "pool" {
			if args[1] == "stats" {
				if args[2] == "replicapool" {
					return a, nil
				}
				return "", errors.Errorf("rbd:error opening pool '%s': (2) No such file or directory", args[3])

			}
		}
		return "", errors.Errorf("unexpected rbd command %q", args)
	}

	clusterInfo := AdminTestClusterInfo("mycluster")
	stats, err := GetPoolStatistics(context, clusterInfo, "replicapool")
	assert.Nil(t, err)
	assert.True(t, reflect.DeepEqual(stats, &p))

	stats, err = GetPoolStatistics(context, clusterInfo, "rbd")
	assert.NotNil(t, err)
	assert.Nil(t, stats)
}

func TestSetPoolReplicatedSizeProperty(t *testing.T) {
	poolName := "mypool"
	executor := &exectest.MockExecutor{}
	context := &clusterd.Context{Executor: executor}
	executor.MockExecuteCommandWithOutput = func(command string, args ...string) (string, error) {
		logger.Infof("Command: %s %v", command, args)

		if args[2] == "set" {
			assert.Equal(t, poolName, args[3])
			assert.Equal(t, "size", args[4])
			assert.Equal(t, "3", args[5])
			return "", nil
		}

		return "", errors.Errorf("unexpected ceph command %q", args)
	}

	err := SetPoolReplicatedSizeProperty(context, AdminTestClusterInfo("mycluster"), poolName, "3")
	assert.NoError(t, err)

	// TEST POOL SIZE 1 AND RequireSafeReplicaSize True
	executor.MockExecuteCommandWithOutput = func(command string, args ...string) (string, error) {
		logger.Infof("Command: %s %v", command, args)

		if args[2] == "set" {
			assert.Equal(t, "mypool", args[3])
			assert.Equal(t, "size", args[4])
			assert.Equal(t, "1", args[5])
			assert.Equal(t, "--yes-i-really-mean-it", args[6])
			return "", nil
		}

		return "", errors.Errorf("unexpected ceph command %q", args)
	}

	err = SetPoolReplicatedSizeProperty(context, AdminTestClusterInfo("mycluster"), poolName, "1")
	assert.NoError(t, err)
}

func TestCreateStretchCrushRule(t *testing.T) {
	testCreateStretchCrushRule(t, true)
	testCreateStretchCrushRule(t, false)
}

func testCreateStretchCrushRule(t *testing.T, alreadyExists bool) {
	executor := &exectest.MockExecutor{}
	context := &clusterd.Context{Executor: executor}
	executor.MockExecuteCommandWithOutput = func(command string, args ...string) (string, error) {
		logger.Infof("Command: %s %v", command, args)
		if args[0] == "osd" {
			if args[1] == "dump" {
				return `{"crush_version": 1}`, nil
			}
			if args[1] == "getcrushmap" {
				return "", nil
			}
			if args[1] == "setcrushmap" {
				if alreadyExists {
					return "", errors.New("setcrushmap not expected for already existing crush rule")
				}
				return "", nil
			}
		}
		if command == "crushtool" {
			switch args[0] {
			case "--decompile", "--compile":
				if alreadyExists {
					return "", errors.New("--compile or --decompile not expected for already existing crush rule")
				}
				return "", nil
			}
		}
		if args[0] == "osd" && args[1] == "crush" && args[2] == "dump" {
			return testCrushMap, nil
		}
		return "", errors.Errorf("unexpected ceph command %q", args)
	}
	clusterInfo := AdminTestClusterInfo("mycluster")
	clusterSpec := &cephv1.ClusterSpec{}
	poolSpec := cephv1.PoolSpec{FailureDomain: "rack"}
	ruleName := "testrule"
	if alreadyExists {
		ruleName = "replicated_ruleset"
	}

	err := createStretchCrushRule(context, clusterInfo, clusterSpec, ruleName, poolSpec)
	assert.NoError(t, err)
}

func TestCreatePoolWithReplicasPerFailureDomain(t *testing.T) {
	// This test goes via the path of explicit compile/decompile CRUSH map; ignored if 'crushtool' is not installed
	// on local build machine
	if hasCrushtool() {
		testCreatePoolWithReplicasPerFailureDomain(t, "host", "mycrushroot", "hdd")
		testCreatePoolWithReplicasPerFailureDomain(t, "rack", "mycrushroot", "ssd")
	}
}

func testCreatePoolWithReplicasPerFailureDomain(t *testing.T, failureDomain, crushRoot, deviceClass string) {
	poolRuleCreated := false
	poolRuleSet := false
	poolAppEnable := false
	poolSpec := cephv1.NamedPoolSpec{
		Name: "mypool-with-two-step-clush-rule",
		PoolSpec: cephv1.PoolSpec{
			FailureDomain: failureDomain,
			CrushRoot:     crushRoot,
			DeviceClass:   deviceClass,
			Replicated: cephv1.ReplicatedSpec{
				Size:                     12345678,
				ReplicasPerFailureDomain: 2,
			},
		},
	}

	executor := &exectest.MockExecutor{}
	executor.MockExecuteCommandWithOutput = func(command string, args ...string) (string, error) {
		logger.Infof("Command: %s %v", command, args)
		assert.Equal(t, command, "ceph")
		assert.Equal(t, args[0], "osd")
		if len(args) >= 3 && args[1] == "crush" && args[2] == "dump" {
			return testCrushMap, nil
		}
		if len(args) >= 3 && args[1] == "pool" && args[2] == "create" {
			// Currently, CRUSH-rule name equals pool's name
			assert.GreaterOrEqual(t, len(args), 7)
			assert.Equal(t, args[3], poolSpec.Name)
			assert.Equal(t, args[5], "replicated")
			crushRuleName := args[6]
			assert.Equal(t, crushRuleName, poolSpec.Name)
			poolRuleCreated = true
			return "", nil
		}
		if len(args) >= 3 && args[1] == "pool" && args[2] == "set" {
			crushRuleName := args[3]
			assert.Equal(t, crushRuleName, poolSpec.Name)
			assert.Equal(t, args[4], "size")
			poolSize, err := strconv.Atoi(args[5])
			assert.NoError(t, err)
			uPoolSize := uint(poolSize) // nolint:gosec // G115 : we know it is not too big
			assert.Equal(t, uPoolSize, poolSpec.Replicated.Size)
			poolRuleSet = true
			return "", nil
		}
		if len(args) >= 4 && args[1] == "pool" && args[2] == "application" {
			if args[3] == "get" {
				return emptyApplicationName, nil
			}

			crushRuleName := args[4]
			assert.Equal(t, crushRuleName, poolSpec.Name)
			poolAppEnable = true
			return "", nil
		}
		if len(args) >= 4 && args[1] == "crush" && args[2] == "rule" && args[3] == "create-replicated" {
			crushRuleName := args[4]
			assert.Equal(t, crushRuleName, poolSpec.Name)
			deviceClassName := args[7]
			assert.Equal(t, deviceClassName, deviceClass)
			poolRuleCreated = true
			return "", nil
		}
		return "", errors.Errorf("unexpected ceph command %q", args)
	}
	context := &clusterd.Context{Executor: executor}
	clusterSpec := &cephv1.ClusterSpec{Storage: cephv1.StorageScopeSpec{Config: map[string]string{CrushRootConfigKey: "cluster-crush-root"}}}
	err := createReplicatedPoolForApp(context, AdminTestClusterInfo("mycluster"), clusterSpec, poolSpec, DefaultPGCount)
	assert.Nil(t, err)
	assert.True(t, poolRuleCreated)
	assert.True(t, poolRuleSet)
	assert.True(t, poolAppEnable)
}

func TestCreateHybridCrushRule(t *testing.T) {
	testCreateHybridCrushRule(t, true)
	testCreateHybridCrushRule(t, false)
}

func testCreateHybridCrushRule(t *testing.T, alreadyExists bool) {
	executor := &exectest.MockExecutor{}
	context := &clusterd.Context{Executor: executor}
	executor.MockExecuteCommandWithOutput = func(command string, args ...string) (string, error) {
		logger.Infof("Command: %s %v", command, args)
		if args[0] == "osd" {
			if args[1] == "dump" {
				return `{"crush_version": 1}`, nil
			}
			if args[1] == "getcrushmap" {
				return "", nil
			}
			if args[1] == "setcrushmap" {
				if alreadyExists {
					return "", errors.New("setcrushmap not expected for already existing crush rule")
				}
				return "", nil
			}
		}
		if command == "crushtool" {
			switch args[0] {
			case "--decompile", "--compile":
				if alreadyExists {
					return "", errors.New("--compile or --decompile not expected for already existing crush rule")
				}
				return "", nil
			}
		}
		if args[0] == "osd" && args[1] == "crush" && args[2] == "dump" {
			return testCrushMap, nil
		}
		return "", errors.Errorf("unexpected ceph command %q", args)
	}
	clusterInfo := AdminTestClusterInfo("mycluster")
	clusterSpec := &cephv1.ClusterSpec{}
	poolSpec := cephv1.PoolSpec{
		FailureDomain: "rack",
		Replicated: cephv1.ReplicatedSpec{
			HybridStorage: &cephv1.HybridStorageSpec{
				PrimaryDeviceClass:   "ssd",
				SecondaryDeviceClass: "hdd",
			},
		},
	}
	ruleName := "testrule"
	if alreadyExists {
		ruleName = "hybrid_ruleset"
	}

	err := createHybridCrushRule(context, clusterInfo, clusterSpec, ruleName, poolSpec)
	assert.NoError(t, err)
}

func hasCrushtool() bool {
	_, err := exec.LookPath("crushtool")
	return err == nil
}

func TestBuildWriteAffinityCrushRule(t *testing.T) {
	clusterSpec := &cephv1.ClusterSpec{}
	pool := cephv1.NamedPoolSpec{
		Name: "mypool",
		PoolSpec: cephv1.PoolSpec{
			FailureDomain: "zone",
			Replicated:    cephv1.ReplicatedSpec{Size: 3, WriteAffinity: &cephv1.WriteAffinitySpec{FailureDomains: []string{"us-east-1a", "us.east.1b", "us-east-1c"}}},
		},
	}
	crushMap := CrushMap{Rules: []ruleSpec{{ID: 0}, {ID: 1}}}

	expected := `
rule mypool_wa_x {
        id 2
        type replicated
        step take us-east-1a
        step chooseleaf firstn 1 type host
        step emit
        step take us-east-1b
        step chooseleaf firstn 1 type host
        step emit
        step take us-east-1c
        step chooseleaf firstn 1 type host
        step emit
}
`
	assert.Equal(t, expected, buildWriteAffinityCrushRule(crushMap, "mypool_wa_x", buildWriteAffinityCrushSteps(clusterSpec, pool.PoolSpec)))

	name := WriteAffinityCrushRuleName(clusterSpec, pool)
	assert.Regexp(t, `^mypool_wa_[0-9a-f]{8}$`, name)
	assert.True(t, isWriteAffinityCrushRule("mypool", name))
	assert.Equal(t, name, WriteAffinityCrushRuleName(clusterSpec, pool), "rule name must be stable")

	t.Run("reordering changes the rule", func(t *testing.T) {
		p := *pool.DeepCopy()
		p.Replicated.WriteAffinity.FailureDomains = []string{"us-east-1b", "us-east-1a", "us-east-1c"}
		assert.NotEqual(t, name, WriteAffinityCrushRuleName(clusterSpec, p))
	})

	t.Run("device class, replicas and sub failure domain", func(t *testing.T) {
		p := *pool.DeepCopy()
		p.DeviceClass = "ssd"
		p.Replicated.ReplicasPerFailureDomain = 2
		p.Replicated.SubFailureDomain = "rack"
		p.Replicated.WriteAffinity.FailureDomains = []string{"a", "b"}
		assert.Equal(t, "        step take a class ssd\n        step chooseleaf firstn 2 type rack\n        step emit\n"+
			"        step take b class ssd\n        step chooseleaf firstn 2 type rack\n        step emit\n", buildWriteAffinityCrushSteps(clusterSpec, p.PoolSpec))
		assert.NotEqual(t, name, WriteAffinityCrushRuleName(clusterSpec, p))
	})

	t.Run("stretch cluster sub failure domain", func(t *testing.T) {
		stretchSpec := &cephv1.ClusterSpec{Mon: cephv1.MonSpec{StretchCluster: &cephv1.StretchClusterSpec{
			SubFailureDomain: "rack",
			Zones:            []cephv1.MonZoneSpec{{Name: "a"}, {Name: "b"}, {Name: "c", Arbiter: true}},
		}}}
		assert.Equal(t, "rack", WriteAffinitySubFailureDomain(stretchSpec, pool.PoolSpec))
		assert.Equal(t, "host", WriteAffinitySubFailureDomain(clusterSpec, pool.PoolSpec))
	})
}

func TestIsWriteAffinityCrushRule(t *testing.T) {
	assert.True(t, isWriteAffinityCrushRule("mypool", "mypool_wa_0123abcd"))
	assert.False(t, isWriteAffinityCrushRule("mypool", "mypool"))
	assert.False(t, isWriteAffinityCrushRule("mypool", "mypool_zone"))
	assert.False(t, isWriteAffinityCrushRule("mypool", "mypool_wa_0123abcdef"))
	assert.False(t, isWriteAffinityCrushRule("mypool", "mypool_wa_xyzxyzxy"))
	assert.False(t, isWriteAffinityCrushRule("my", "mypool_wa_0123abcd"))
	assert.False(t, isWriteAffinityCrushRule("mypool", "mypool_wa_b_wa_0123abcd"))
}

// writeAffinityTestExecutor mocks the ceph commands of a replicated pool reconcile and records the commands
// that change the cluster
type writeAffinityTestExecutor struct {
	currentRule  string
	size         int
	stretchMode  bool
	sizeSet      []string
	order        []string
	poolCreated  []string
	ruleSet      []string
	rulesRemoved []string
	rulesCreated []string
	crushMapSet  int
	// crushMapChanges is the number of crush map injections that fail with a changed crush version
	crushMapChanges int
}

func (e *writeAffinityTestExecutor) context(t *testing.T) *clusterd.Context {
	executor := &exectest.MockExecutor{}
	executor.MockExecuteCommandWithOutput = func(command string, args ...string) (string, error) {
		logger.Infof("Command: %s %v", command, args)
		if command == "crushtool" {
			return "", nil
		}
		switch {
		case args[0] == "mon" && args[1] == "dump":
			return fmt.Sprintf(`{"stretch_mode": %t}`, e.stretchMode), nil
		case args[0] == "osd" && args[1] == "dump":
			return fmt.Sprintf(`{"crush_version": %d}`, 5+e.crushMapSet), nil
		case args[1] == "getcrushmap":
			return "", nil
		case args[1] == "setcrushmap":
			if args[4] != fmt.Sprint(5+e.crushMapSet) {
				return "", errors.Errorf("unexpected prior_version %q", args[4])
			}
			if e.crushMapChanges > 0 {
				e.crushMapChanges--
				return "", errors.New("Error EPERM: prior_version 5 != crush version 6")
			}
			e.crushMapSet++
			return "", nil
		case args[1] == "crush" && args[2] == "dump":
			return testCrushMap, nil
		case args[1] == "crush" && args[2] == "rule" && args[3] == "rm":
			e.rulesRemoved = append(e.rulesRemoved, args[4])
			return "", nil
		case args[1] == "crush" && args[2] == "rule" && args[3] == "create-replicated":
			e.rulesCreated = append(e.rulesCreated, args[4])
			return "", nil
		case args[1] == "crush" && args[2] == "rule" && args[3] == "dump":
			if isWriteAffinityCrushRule("mypool", args[4]) {
				return `{"steps": [{"op":"take","item_name":"a"},{"op":"chooseleaf_firstn","type":"host"},{"op":"emit"}]}`, nil
			}
			return `{"steps": [{"op":"take","item_name":"default"},{"op":"chooseleaf_firstn","type":"zone"},{"op":"emit"}]}`, nil
		case args[1] == "pool" && args[2] == "get":
			if e.currentRule == "" {
				return "", errors.New("pool does not exist")
			}
			size := e.size
			if size == 0 {
				size = 3
			}
			return fmt.Sprintf(`{"pool":"mypool","pool_id":1,"size":%d,"crush_rule":%q}`, size, e.currentRule), nil
		case args[1] == "pool" && args[2] == "create":
			e.poolCreated = append(e.poolCreated, args[6])
			return "", nil
		case args[1] == "pool" && args[2] == "set" && args[4] == "crush_rule":
			e.ruleSet = append(e.ruleSet, args[5])
			e.order = append(e.order, "crush_rule")
			e.currentRule = args[5]
			return "", nil
		case args[1] == "pool" && args[2] == "set" && args[4] == "size":
			e.sizeSet = append(e.sizeSet, args[5])
			e.order = append(e.order, "size")
			return "", nil
		case args[1] == "pool" && args[2] == "set":
			return "", nil
		case args[1] == "pool" && args[2] == "application":
			if args[3] == "get" {
				return emptyApplicationName, nil
			}
			return "", nil
		}
		return "", errors.Errorf("unexpected command %s %q", command, args)
	}
	return &clusterd.Context{Executor: executor}
}

func TestCreateWriteAffinityPool(t *testing.T) {
	enable := true
	newPool := func() cephv1.NamedPoolSpec {
		return cephv1.NamedPoolSpec{
			Name: "mypool",
			PoolSpec: cephv1.PoolSpec{
				FailureDomain: "zone",
				Replicated:    cephv1.ReplicatedSpec{Size: 3, WriteAffinity: &cephv1.WriteAffinitySpec{FailureDomains: []string{"a", "b", "c"}}},
			},
		}
	}
	clusterSpec := &cephv1.ClusterSpec{}
	stretchSpec := &cephv1.ClusterSpec{Mon: cephv1.MonSpec{StretchCluster: &cephv1.StretchClusterSpec{
		Zones: []cephv1.MonZoneSpec{{Name: "a"}, {Name: "b"}, {Name: "c", Arbiter: true}},
	}}}
	desiredRule := WriteAffinityCrushRuleName(clusterSpec, newPool())

	t.Run("new pool", func(t *testing.T) {
		e := &writeAffinityTestExecutor{}
		err := createReplicatedPoolForApp(e.context(t), AdminTestClusterInfo("mycluster"), clusterSpec, newPool(), DefaultPGCount)
		assert.NoError(t, err)
		assert.Equal(t, []string{desiredRule}, e.poolCreated)
		assert.Equal(t, 1, e.crushMapSet)
		assert.Empty(t, e.ruleSet)
		assert.Empty(t, e.rulesCreated)
	})

	t.Run("new pool retries when the crush map changed", func(t *testing.T) {
		e := &writeAffinityTestExecutor{crushMapChanges: 1}
		err := createReplicatedPoolForApp(e.context(t), AdminTestClusterInfo("mycluster"), clusterSpec, newPool(), DefaultPGCount)
		assert.NoError(t, err)
		assert.Equal(t, 1, e.crushMapSet)
		assert.Equal(t, []string{desiredRule}, e.poolCreated)

		e = &writeAffinityTestExecutor{crushMapChanges: crushMapUpdateAttempts}
		err = createReplicatedPoolForApp(e.context(t), AdminTestClusterInfo("mycluster"), clusterSpec, newPool(), DefaultPGCount)
		assert.ErrorIs(t, err, errCrushMapChanged)
		assert.Empty(t, e.poolCreated)
	})

	t.Run("existing pool with the desired rule", func(t *testing.T) {
		e := &writeAffinityTestExecutor{currentRule: desiredRule}
		p := newPool()
		p.EnableCrushUpdates = &enable
		p.DeviceClass = ""
		err := createReplicatedPoolForApp(e.context(t), AdminTestClusterInfo("mycluster"), clusterSpec, p, DefaultPGCount)
		assert.NoError(t, err)
		assert.Empty(t, e.poolCreated)
		assert.Empty(t, e.ruleSet)
		assert.Empty(t, e.rulesRemoved)
	})

	t.Run("existing pool without enableCrushUpdates keeps its rule", func(t *testing.T) {
		e := &writeAffinityTestExecutor{currentRule: "mypool"}
		err := createReplicatedPoolForApp(e.context(t), AdminTestClusterInfo("mycluster"), clusterSpec, newPool(), DefaultPGCount)
		assert.NoError(t, err)
		assert.Empty(t, e.ruleSet)
		assert.Empty(t, e.rulesRemoved)
		assert.Empty(t, e.rulesCreated)
		assert.Equal(t, 0, e.crushMapSet, "the rule is only created when it can be applied")
	})

	t.Run("changed list without enableCrushUpdates does not create the rule", func(t *testing.T) {
		e := &writeAffinityTestExecutor{currentRule: "mypool_wa_0123abcd"}
		err := createReplicatedPoolForApp(e.context(t), AdminTestClusterInfo("mycluster"), clusterSpec, newPool(), DefaultPGCount)
		assert.NoError(t, err)
		assert.Equal(t, 0, e.crushMapSet)
		assert.Empty(t, e.ruleSet)
		assert.Empty(t, e.rulesRemoved)
	})

	t.Run("size change without enableCrushUpdates is rejected while the old rule is kept", func(t *testing.T) {
		e := &writeAffinityTestExecutor{currentRule: "mypool_wa_0123abcd"}
		p := newPool()
		p.Replicated = cephv1.ReplicatedSpec{Size: 4, WriteAffinity: &cephv1.WriteAffinitySpec{FailureDomains: []string{"a", "b", "c", "d"}}}
		err := createReplicatedPoolForApp(e.context(t), AdminTestClusterInfo("mycluster"), clusterSpec, p, DefaultPGCount)
		assert.ErrorContains(t, err, "cannot change the size of pool")
		assert.Empty(t, e.sizeSet)
		assert.Empty(t, e.ruleSet)

		// the size changes together with the rule when crush updates are enabled
		p.EnableCrushUpdates = &enable
		err = createReplicatedPoolForApp(e.context(t), AdminTestClusterInfo("mycluster"), clusterSpec, p, DefaultPGCount)
		assert.NoError(t, err)
		assert.Equal(t, []string{"4"}, e.sizeSet)
		assert.Equal(t, []string{WriteAffinityCrushRuleName(clusterSpec, p)}, e.ruleSet)
		assert.Equal(t, []string{"crush_rule", "size"}, e.order, "the wider rule is set before the size")
	})

	t.Run("size decrease sets the rule after the size", func(t *testing.T) {
		e := &writeAffinityTestExecutor{currentRule: "mypool_wa_0123abcd", size: 4}
		p := newPool()
		p.EnableCrushUpdates = &enable
		err := createReplicatedPoolForApp(e.context(t), AdminTestClusterInfo("mycluster"), clusterSpec, p, DefaultPGCount)
		assert.NoError(t, err)
		assert.Equal(t, []string{"size", "crush_rule"}, e.order)
	})

	t.Run("existing pool with enableCrushUpdates switches rule", func(t *testing.T) {
		e := &writeAffinityTestExecutor{currentRule: "mypool"}
		p := newPool()
		p.EnableCrushUpdates = &enable
		err := createReplicatedPoolForApp(e.context(t), AdminTestClusterInfo("mycluster"), clusterSpec, p, DefaultPGCount)
		assert.NoError(t, err)
		assert.Equal(t, []string{desiredRule}, e.ruleSet)
		assert.Empty(t, e.rulesRemoved, "rules not created for write affinity are left to the crush rule cleanup")
	})

	t.Run("changed list removes the previous write affinity rule", func(t *testing.T) {
		e := &writeAffinityTestExecutor{currentRule: "mypool_wa_0123abcd"}
		p := newPool()
		p.EnableCrushUpdates = &enable
		err := createReplicatedPoolForApp(e.context(t), AdminTestClusterInfo("mycluster"), clusterSpec, p, DefaultPGCount)
		assert.NoError(t, err)
		assert.Equal(t, []string{desiredRule}, e.ruleSet)
		assert.Equal(t, []string{"mypool_wa_0123abcd"}, e.rulesRemoved)
	})

	t.Run("stretch cluster waits for stretch mode", func(t *testing.T) {
		e := &writeAffinityTestExecutor{}
		p := newPool()
		p.Replicated = cephv1.ReplicatedSpec{Size: 4, ReplicasPerFailureDomain: 2, WriteAffinity: &cephv1.WriteAffinitySpec{FailureDomains: []string{"a", "b"}}}
		err := createReplicatedPoolForApp(e.context(t), AdminTestClusterInfo("mycluster"), stretchSpec, p, DefaultPGCount)
		assert.ErrorContains(t, err, "waiting for stretch mode to be enabled")
		assert.Empty(t, e.poolCreated)

		e.stretchMode = true
		err = createReplicatedPoolForApp(e.context(t), AdminTestClusterInfo("mycluster"), stretchSpec, p, DefaultPGCount)
		assert.NoError(t, err)
		assert.Equal(t, []string{WriteAffinityCrushRuleName(stretchSpec, p)}, e.poolCreated)
	})

	t.Run("removing writeAffinity in a stretch cluster restores the stretch rule", func(t *testing.T) {
		e := &writeAffinityTestExecutor{currentRule: "mypool_wa_0123abcd"}
		p := newPool()
		p.Replicated = cephv1.ReplicatedSpec{Size: 4}
		p.EnableCrushUpdates = &enable
		err := createReplicatedPoolForApp(e.context(t), AdminTestClusterInfo("mycluster"), stretchSpec, p, DefaultPGCount)
		assert.NoError(t, err)
		assert.Equal(t, []string{defaultStretchCrushRuleName}, e.ruleSet)
		assert.Equal(t, []string{"mypool_wa_0123abcd"}, e.rulesRemoved)
	})

	t.Run("removing writeAffinity without enableCrushUpdates keeps the rule", func(t *testing.T) {
		e := &writeAffinityTestExecutor{currentRule: "mypool_wa_0123abcd"}
		p := newPool()
		p.Replicated = cephv1.ReplicatedSpec{Size: 4}
		err := createReplicatedPoolForApp(e.context(t), AdminTestClusterInfo("mycluster"), stretchSpec, p, DefaultPGCount)
		assert.NoError(t, err)
		assert.Empty(t, e.ruleSet)
		assert.Empty(t, e.rulesRemoved)
	})

	t.Run("removing writeAffinity with replicasPerFailureDomain recreates the two-step rule", func(t *testing.T) {
		e := &writeAffinityTestExecutor{currentRule: "mypool_wa_0123abcd"}
		p := newPool()
		p.Replicated = cephv1.ReplicatedSpec{Size: 4, ReplicasPerFailureDomain: 2}
		p.EnableCrushUpdates = &enable
		err := createReplicatedPoolForApp(e.context(t), AdminTestClusterInfo("mycluster"), clusterSpec, p, DefaultPGCount)
		assert.NoError(t, err)
		assert.Equal(t, []string{"mypool", "mypool_wa_0123abcd"}, e.rulesRemoved)
		assert.Equal(t, 1, e.crushMapSet)
		assert.Equal(t, []string{"mypool"}, e.ruleSet)
	})

	t.Run("removing writeAffinity from a plain replicated pool", func(t *testing.T) {
		e := &writeAffinityTestExecutor{currentRule: "mypool_wa_0123abcd"}
		p := newPool()
		p.Replicated = cephv1.ReplicatedSpec{Size: 3}
		p.EnableCrushUpdates = &enable
		err := createReplicatedPoolForApp(e.context(t), AdminTestClusterInfo("mycluster"), clusterSpec, p, DefaultPGCount)
		assert.NoError(t, err)
		assert.Equal(t, []string{"mypool_zone"}, e.rulesCreated)
		assert.Equal(t, []string{"mypool_zone"}, e.ruleSet)
		assert.Equal(t, []string{"mypool_zone", "mypool_wa_0123abcd"}, e.rulesRemoved, "the target rule is recreated")
	})

	t.Run("removing writeAffinity from a pool without failureDomain", func(t *testing.T) {
		e := &writeAffinityTestExecutor{currentRule: "mypool_wa_0123abcd"}
		p := newPool()
		p.FailureDomain = ""
		p.Replicated = cephv1.ReplicatedSpec{Size: 3}
		p.EnableCrushUpdates = &enable
		err := createReplicatedPoolForApp(e.context(t), AdminTestClusterInfo("mycluster"), clusterSpec, p, DefaultPGCount)
		assert.NoError(t, err)
		assert.Equal(t, []string{"mypool"}, e.rulesCreated)
		assert.Equal(t, []string{"mypool"}, e.ruleSet)
		assert.Equal(t, []string{"mypool", "mypool_wa_0123abcd"}, e.rulesRemoved)
	})
}
