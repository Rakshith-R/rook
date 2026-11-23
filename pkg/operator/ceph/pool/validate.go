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
	"slices"
	"strings"

	"github.com/pkg/errors"
	cephv1 "github.com/rook/rook/pkg/apis/ceph.rook.io/v1"
	"github.com/rook/rook/pkg/clusterd"
	cephclient "github.com/rook/rook/pkg/daemon/ceph/client"
	"github.com/rook/rook/pkg/operator/ceph/cluster/mon"
	"github.com/rook/rook/pkg/util/log"
)

// validatePool validates the pool arguments
func validatePool(context *clusterd.Context, clusterInfo *cephclient.ClusterInfo, clusterSpec *cephv1.ClusterSpec, p *cephv1.CephBlockPool) error {
	if p.Name == "" {
		return errors.New("missing name")
	}
	if p.Namespace == "" {
		return errors.New("missing namespace")
	}

	if err := cephv1.ValidateCephBlockPool(p); err != nil {
		return err
	}

	if err := validatePoolSpec(context, clusterInfo, clusterSpec, &p.Spec.PoolSpec); err != nil {
		return err
	}
	return nil
}

// ValidatePoolSpec validates the spec of a pool of a CephFilesystem, CephObjectStore or CephObjectZone
func ValidatePoolSpec(context *clusterd.Context, clusterInfo *cephclient.ClusterInfo, clusterSpec *cephv1.ClusterSpec, p *cephv1.PoolSpec) error {
	// There is no client side zone pinning for these pools
	if p.IsWriteAffinityPool() {
		return errors.New("writeAffinity is only supported for CephBlockPool")
	}
	return validatePoolSpec(context, clusterInfo, clusterSpec, p)
}

// validatePoolSpec validates the spec of any pool
func validatePoolSpec(context *clusterd.Context, clusterInfo *cephclient.ClusterInfo, clusterSpec *cephv1.ClusterSpec, p *cephv1.PoolSpec) error {
	if p.IsHybridStoragePool() {
		err := validateDeviceClasses(context, clusterInfo, p)
		if err != nil {
			return errors.Wrap(err, "failed to validate device classes for hybrid storage pool spec")
		}
	}

	if p.IsReplicated() && p.IsErasureCoded() {
		return errors.New("both replication and erasure code settings cannot be specified")
	}

	if p.FailureDomain != "" && p.Replicated.SubFailureDomain != "" {
		if p.FailureDomain == p.Replicated.SubFailureDomain {
			return errors.New("failure and subfailure domain cannot be identical")
		}
	}

	// validate pools for stretch clusters
	if clusterSpec.IsStretchCluster() {
		if p.IsReplicated() {
			if p.Replicated.Size != 4 {
				return errors.New("pools in a stretch cluster must have replication size 4")
			}
		}
		if p.IsErasureCoded() {
			return errors.New("erasure coded pools are not supported in stretch clusters")
		}
	}

	var crush cephclient.CrushMap
	var err error
	if p.FailureDomain != "" || p.CrushRoot != "" {
		crush, err = cephclient.GetCrushMap(context, clusterInfo)
		if err != nil {
			return errors.Wrap(err, "failed to get crush map")
		}
	}

	// validate the failure domain if specified
	if p.FailureDomain != "" {
		found := false
		for _, t := range crush.Types {
			if t.Name == p.FailureDomain {
				found = true
				break
			}
		}
		if !found {
			return errors.Errorf("unrecognized failure domain %s", p.FailureDomain)
		}
	}

	// validate the crush root if specified
	if p.CrushRoot != "" {
		found := false
		for _, t := range crush.Buckets {
			if t.Name == p.CrushRoot {
				found = true
				break
			}
		}
		if !found {
			return errors.Errorf("unrecognized crush root %s", p.CrushRoot)
		}
	}

	// validate the crush subdomain if specified
	if p.Replicated.SubFailureDomain != "" {
		found := false
		for _, t := range crush.Types {
			if t.Name == p.Replicated.SubFailureDomain {
				found = true
				break
			}
		}
		if !found {
			return errors.Errorf("unrecognized crush sub domain %s", p.Replicated.SubFailureDomain)
		}
	}

	if p.IsWriteAffinityPool() {
		if err := validateWriteAffinity(clusterSpec, p, crush); err != nil {
			return errors.Wrap(err, "invalid writeAffinity")
		}
	}

	// validate pool replica size
	if p.IsReplicated() {
		if p.Replicated.Size == 1 && p.Replicated.RequireSafeReplicaSize {
			return errors.Errorf("error pool size is %d and requireSafeReplicaSize is %t, must be false", p.Replicated.Size, p.Replicated.RequireSafeReplicaSize)
		}

		if p.Replicated.Size <= p.Replicated.ReplicasPerFailureDomain {
			return errors.Errorf("error pool size is %d and replicasPerFailureDomain is %d, size must be greater", p.Replicated.Size, p.Replicated.ReplicasPerFailureDomain)
		}

		if p.Replicated.ReplicasPerFailureDomain != 0 && p.Replicated.Size%p.Replicated.ReplicasPerFailureDomain != 0 {
			return errors.Errorf("error replicasPerFailureDomain is %d must be a factor of the replica count %d", p.Replicated.ReplicasPerFailureDomain, p.Replicated.Size)
		}
	}

	// validate pool compression mode if specified
	if p.CompressionMode != "" {
		log.NamespacedWarning(clusterInfo.Namespace, logger, "compressionMode is DEPRECATED, use Parameters instead")
	}

	// Test the same for Parameters
	if p.Parameters != nil {
		compression, ok := p.Parameters[cephclient.CompressionModeProperty]
		if ok && compression != "" {
			switch compression {
			case "none", "passive", "aggressive", "force":
				break
			default:
				return errors.Errorf("failed to validate pool spec unknown compression mode %q", compression)
			}
		}
	}

	// Validate mirroring settings
	if p.Mirroring.Enabled {
		switch p.Mirroring.Mode {
		case "image", "pool", "init-only":
			break
		default:
			return errors.Errorf("unrecognized mirroring mode %q. only 'image and 'pool' are supported", p.Mirroring.Mode)
		}

		if p.Mirroring.SnapshotSchedulesEnabled() {
			for _, snapSchedule := range p.Mirroring.SnapshotSchedules {
				if snapSchedule.Interval == "" && snapSchedule.StartTime != "" {
					return errors.New("schedule interval cannot be empty if start time is specified")
				}
			}
		}
	}

	if !p.Mirroring.Enabled && p.Mirroring.SnapshotSchedulesEnabled() {
		log.NamespacedWarning(clusterInfo.Namespace, logger, "mirroring must be enabled to configure snapshot scheduling")
	}

	return nil
}

// validateDeviceClasses validates the primary and secondary device classes in the HybridStorageSpec
func validateDeviceClasses(context *clusterd.Context, clusterInfo *cephclient.ClusterInfo, p *cephv1.PoolSpec) error {
	primaryDeviceClass := p.Replicated.HybridStorage.PrimaryDeviceClass
	secondaryDeviceClass := p.Replicated.HybridStorage.SecondaryDeviceClass

	err := validateDeviceClassOSDs(context, clusterInfo, primaryDeviceClass)
	if err != nil {
		return errors.Wrapf(err, "failed to validate primary device class %q", primaryDeviceClass)
	}

	err = validateDeviceClassOSDs(context, clusterInfo, secondaryDeviceClass)
	if err != nil {
		return errors.Wrapf(err, "failed to validate secondary device class %q", secondaryDeviceClass)
	}

	return nil
}

// validateDeviceClassOSDs validates that the device class should have at least one OSD
func validateDeviceClassOSDs(context *clusterd.Context, clusterInfo *cephclient.ClusterInfo, deviceClassName string) error {
	deviceClassOSDs, err := cephclient.GetDeviceClassOSDs(context, clusterInfo, deviceClassName)
	if err != nil {
		return errors.Wrapf(err, "failed to get osds for the device class %q", deviceClassName)
	}
	if len(deviceClassOSDs) == 0 {
		return errors.Errorf("no osds available for the device class %q", deviceClassName)
	}

	return nil
}

// validateWriteAffinity validates the writeAffinity settings against the cluster spec and the crush map. The
// crush map is only populated when the pool failureDomain is set.
func validateWriteAffinity(clusterSpec *cephv1.ClusterSpec, p *cephv1.PoolSpec, crush cephclient.CrushMap) error {
	if !p.IsReplicated() || p.IsErasureCoded() {
		return errors.New("writeAffinity requires a replicated pool")
	}
	if p.IsHybridStoragePool() {
		return errors.New("writeAffinity and hybridStorage cannot be specified together")
	}
	if p.FailureDomain == "" {
		return errors.New("failureDomain must be set")
	}
	subFailureDomain := cephclient.WriteAffinitySubFailureDomain(clusterSpec, *p)
	if subFailureDomain == p.FailureDomain {
		return errors.Errorf("failureDomain and subFailureDomain %q cannot be identical", subFailureDomain)
	}
	failureDomains := p.Replicated.WriteAffinity.FailureDomains
	replicas := max(p.Replicated.ReplicasPerFailureDomain, 1)
	if p.Replicated.Size != uint(len(failureDomains))*replicas {
		return errors.Errorf("size %d must equal the number of failureDomains (%d) multiplied by replicasPerFailureDomain (%d)", p.Replicated.Size, len(failureDomains), replicas)
	}

	if clusterSpec.IsStretchCluster() {
		if err := validateStretchWriteAffinity(clusterSpec, p); err != nil {
			return err
		}
	}

	crushRoot := p.CrushRoot
	if crushRoot == "" {
		crushRoot = cephclient.GetCrushRootFromSpec(clusterSpec)
	}
	underRoot := crushSubtree(crush, crushRoot)
	seen := map[string]bool{}
	for _, failureDomain := range failureDomains {
		name := cephclient.NormalizeCrushName(failureDomain)
		if seen[name] {
			return errors.Errorf("failure domain %q is listed more than once", failureDomain)
		}
		seen[name] = true

		bucket := findCrushBucket(crush, name)
		if bucket == nil || bucket.TypeName != p.FailureDomain {
			return errors.Errorf("crush bucket %q of type %q not found", name, p.FailureDomain)
		}
		if !underRoot[bucket.ID] {
			return errors.Errorf("crush bucket %q is not under the crush root %q", name, crushRoot)
		}
		subtree := crushSubtree(crush, name)
		if p.DeviceClass != "" && !hasDeviceClass(crush, subtree, p.DeviceClass) {
			return errors.Errorf("crush bucket %q has no OSDs of device class %q", name, p.DeviceClass)
		}
		if count := countCrushBuckets(crush, subtree, subFailureDomain); count < int(replicas) {
			return errors.Errorf("crush bucket %q has %d buckets of type %q, at least %d are required", name, count, subFailureDomain, replicas)
		}
	}
	return nil
}

// validateStretchWriteAffinity checks that a write affinity pool in a stretch cluster spans both data zones
func validateStretchWriteAffinity(clusterSpec *cephv1.ClusterSpec, p *cephv1.PoolSpec) error {
	// PGs of a device class rule fail to peer in degraded stretch mode
	if p.DeviceClass != "" {
		return errors.New("deviceClass is not supported in a stretch cluster")
	}

	// the failure domain of the mons is the type of the stretch zones in the crush map
	stretchFailureDomain := mon.GetFailureDomainLabel(*clusterSpec)
	if _, after, found := strings.Cut(stretchFailureDomain, "/"); found {
		stretchFailureDomain = after
	}
	if p.FailureDomain != stretchFailureDomain {
		return errors.Errorf("failureDomain must be the stretch cluster failure domain %q", stretchFailureDomain)
	}

	dataZones := []string{}
	for _, zone := range clusterSpec.Mon.StretchCluster.Zones {
		if !zone.Arbiter {
			dataZones = append(dataZones, cephclient.NormalizeCrushName(zone.Name))
		}
	}
	failureDomains := []string{}
	for _, failureDomain := range p.Replicated.WriteAffinity.FailureDomains {
		failureDomains = append(failureDomains, cephclient.NormalizeCrushName(failureDomain))
	}
	slices.Sort(dataZones)
	slices.Sort(failureDomains)
	if !slices.Equal(dataZones, failureDomains) {
		return errors.Errorf("failureDomains must be the non-arbiter stretch cluster zones %v", dataZones)
	}
	return nil
}

func findCrushBucket(crush cephclient.CrushMap, name string) *cephclient.CrushBucket {
	for i := range crush.Buckets {
		if crush.Buckets[i].Name == name {
			return &crush.Buckets[i]
		}
	}
	return nil
}

// crushSubtree returns the IDs of all buckets and devices below the named bucket, including the bucket itself
func crushSubtree(crush cephclient.CrushMap, name string) map[int]bool {
	subtree := map[int]bool{}
	bucket := findCrushBucket(crush, name)
	if bucket == nil {
		return subtree
	}
	byID := map[int]*cephclient.CrushBucket{}
	for i := range crush.Buckets {
		byID[crush.Buckets[i].ID] = &crush.Buckets[i]
	}
	queue := []*cephclient.CrushBucket{bucket}
	subtree[bucket.ID] = true
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for _, item := range current.Items {
			if subtree[item.ID] {
				continue
			}
			subtree[item.ID] = true
			if child, ok := byID[item.ID]; ok {
				queue = append(queue, child)
			}
		}
	}
	return subtree
}

// countCrushBuckets returns the number of buckets of the given type in the subtree. A type of "osd" counts devices.
func countCrushBuckets(crush cephclient.CrushMap, subtree map[int]bool, typeName string) int {
	count := 0
	if typeName == "osd" {
		for _, device := range crush.Devices {
			if subtree[device.ID] {
				count++
			}
		}
		return count
	}
	for _, bucket := range crush.Buckets {
		if subtree[bucket.ID] && bucket.TypeName == typeName {
			count++
		}
	}
	return count
}

func hasDeviceClass(crush cephclient.CrushMap, subtree map[int]bool, deviceClass string) bool {
	for _, device := range crush.Devices {
		if subtree[device.ID] && device.Class == deviceClass {
			return true
		}
	}
	return false
}
