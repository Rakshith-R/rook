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

package osd

import (
	"fmt"
	"path/filepath"

	"github.com/pkg/errors"
	cephv1 "github.com/rook/rook/pkg/apis/ceph.rook.io/v1"
	"github.com/rook/rook/pkg/operator/ceph/controller"
	"github.com/rook/rook/pkg/operator/k8sutil"
	batch "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrl "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func keyRotationCronJobName(osdID int) string {
	return fmt.Sprintf(keyRotationCronJobAppNameFmt, osdID)
}

func (c *Cluster) makeKeyRotationCronJob(pvcName string, osd OSDInfo, osdProps osdProperties) (*batch.CronJob, error) {
	podSpec, err := c.keyRotationPodTemplateSpec(osdProps, osd, v1.RestartPolicyOnFailure)
	if err != nil {
		return nil, err
	}

	cronJob := &batch.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      keyRotationCronJobName(osd.ID),
			Namespace: c.clusterInfo.Namespace,
			Labels: map[string]string{
				k8sutil.AppAttr:     keyRotationCronJobAppName,
				k8sutil.ClusterAttr: c.clusterInfo.Namespace,
			},
		},
		Spec: batch.CronJobSpec{
			ConcurrencyPolicy: batch.ForbidConcurrent,
			Schedule:          c.spec.Security.KeyManagementService.Schedule,
			JobTemplate: batch.JobTemplateSpec{
				Spec: batch.JobSpec{
					Template: *podSpec,
				},
			},
		},
	}
	// err = c.clusterInfo.OwnerInfo.SetOwnerReference(cronJob)
	// if err != nil {
	// 	return nil, errors.Wrapf(err, "failed to set owner reference on key rotation cron job %q", cronJob.Name)
	// }
	k8sutil.AddRookVersionLabelToCronJob(cronJob)
	// override the resources of all the init containers and main container with the expected osd prepare resources
	c.applyResourcesToAllContainers(&podSpec.Spec, cephv1.GetPrepareOSDResources(c.spec.Resources))
	return cronJob, nil
}

func (c *Cluster) keyRotationPodTemplateSpec(osdProps osdProperties, osd OSDInfo, restart v1.RestartPolicy) (*v1.PodTemplateSpec, error) {
	// create a volume on /dev so the pod can access devices on the host
	devVolume := v1.Volume{Name: "devices", VolumeSource: v1.VolumeSource{HostPath: &v1.HostPathVolumeSource{Path: "/dev"}}}
	udevVolume := v1.Volume{Name: "udev", VolumeSource: v1.VolumeSource{HostPath: &v1.HostPathVolumeSource{Path: "/run/udev"}}}
	hostPathType := v1.HostPathDirectory
	hostPath := filepath.Join(c.spec.DataDirHostPath, c.clusterInfo.Namespace, osdProps.pvc.ClaimName, fmt.Sprintf("ceph-%d", osd.ID))
	hostPathVolume := v1.Volume{
		Name: "bridge",
		VolumeSource: v1.VolumeSource{
			HostPath: &v1.HostPathVolumeSource{
				Path: hostPath,
				Type: &hostPathType,
			},
		},
	}
	devicesBasePath := "/var/lib/ceph/osd/ceph/"
	volumes := []v1.Volume{
		udevVolume,
		devVolume,
		hostPathVolume,
	}
	volumeMounts := []v1.VolumeMount{
		{Name: "devices", MountPath: "/dev"},
		{Name: "udev", MountPath: "/run/udev"},
		{Name: "bridge", MountPath: devicesBasePath},
	}

	devices := []string{encryptionBlockDestinationCopy(devicesBasePath, bluestoreBlockName)}
	if osdProps.metadataPVC.ClaimName != "" {
		devices = append(devices, encryptionBlockDestinationCopy(devicesBasePath, bluestoreMetadataName))
	}
	if osdProps.walPVC.ClaimName != "" {
		devices = append(devices, encryptionBlockDestinationCopy(devicesBasePath, bluestoreWalName))
	}

	keyRotationContainer, err := c.getKeyRotationContainer(osdProps, volumeMounts, devices)
	if err != nil {
		return nil, errors.Wrap(err, "failed to generate key rotation container")
	}

	podSpec := v1.PodSpec{
		ServiceAccountName: serviceAccountName,
		Containers: []v1.Container{
			keyRotationContainer,
		},
		RestartPolicy:     restart,
		Volumes:           volumes,
		HostNetwork:       c.spec.Network.IsHost(),
		PriorityClassName: cephv1.GetOSDPriorityClassName(c.spec.PriorityClassNames),
		SchedulerName:     osdProps.schedulerName,
	}
	if c.spec.Network.IsHost() {
		podSpec.DNSPolicy = v1.DNSClusterFirstWithHostNet
	}

	// Replace default unreachable node toleration if the osd pod is portable and based in PVC
	if osdProps.portable {
		k8sutil.AddUnreachableNodeToleration(&podSpec)
	}
	osdProps.placement.ApplyToPodSpec(&podSpec)

	c.applyAllPlacementIfNeeded(&podSpec)
	// apply storageClassDeviceSets.preparePlacement
	osdProps.getPreparePlacement().ApplyToPodSpec(&podSpec)

	k8sutil.RemoveDuplicateEnvVars(&podSpec)

	podMeta := metav1.ObjectMeta{
		Name: AppName,
		Labels: map[string]string{
			k8sutil.AppAttr:     keyRotationCronJobAppName,
			k8sutil.ClusterAttr: c.clusterInfo.Namespace,
		},
		Annotations: map[string]string{},
	}
	if c.spec.Network.IsMultus() {
		if err := k8sutil.ApplyMultus(c.spec.Network, &podMeta); err != nil {
			return nil, err
		}
	}
	cephv1.GetKeyRotationAnnotations(c.spec.Annotations).ApplyToObjectMeta(&podMeta)
	cephv1.GetKeyRotationLabels(c.spec.Labels).ApplyToObjectMeta(&podMeta)
	c.applyOSDAffinity(&podSpec, osd, osdProps)

	// cryptsetup synchronizes with udev on host through semaphore
	podSpec.HostIPC = true

	return &v1.PodTemplateSpec{
		ObjectMeta: podMeta,
		Spec:       podSpec,
	}, nil
}

func (c *Cluster) getKeyRotationContainer(osdProps osdProperties, volumeMounts []v1.VolumeMount, devices []string) (v1.Container, error) {
	envVars := c.getConfigEnvVars(osdProps, k8sutil.DataDir, true)

	// enable debug logging
	envVars = append(envVars, setDebugLogLevelEnvVar(true))
	envVars = append(envVars, v1.EnvVar{Name: "ROOK_CEPH_VERSION", Value: c.clusterInfo.CephVersion.CephVersionFormatted()})

	args := []string{osdProps.pvc.ClaimName}
	args = append(args, devices...)

	// run privileged always since we always mount /dev
	privileged := true
	runAsUser := int64(0)
	runAsNonRoot := false
	readOnlyRootFilesystem := false

	osdProvisionContainer := v1.Container{
		Command:         []string{"rook", "key-management", "rotate-key"},
		Args:            args,
		Name:            keyRotationCronJobAppName,
		Image:           c.rookVersion,
		ImagePullPolicy: controller.GetContainerImagePullPolicy(c.spec.CephVersion.ImagePullPolicy),
		VolumeMounts:    volumeMounts,
		Env:             envVars,
		EnvFrom:         getEnvFromSources(),
		SecurityContext: &v1.SecurityContext{
			Privileged:             &privileged,
			RunAsUser:              &runAsUser,
			RunAsNonRoot:           &runAsNonRoot,
			ReadOnlyRootFilesystem: &readOnlyRootFilesystem,
		},
		Resources: cephv1.GetPrepareOSDResources(c.spec.Resources),
	}

	return osdProvisionContainer, nil
}

func (c *Cluster) reconcileKeyRotationCronJob() error {
	selector := labels.SelectorFromSet(map[string]string{k8sutil.AppAttr: keyRotationCronJobAppName})
	listOpt := &client.ListOptions{Namespace: c.clusterInfo.Namespace, LabelSelector: selector}
	if !c.spec.Security.KeyManagementService.EnableKeyRotation {
		err := c.context.Client.DeleteAllOf(c.clusterInfo.Context, &batch.CronJob{}, &client.DeleteAllOfOptions{ListOptions: *listOpt})
		if client.IgnoreNotFound(err) != nil {
			return errors.Wrap(err, "failed to delete key rotation cron jobs")
		}
		logger.Infof("successfully deleted key rotation cron jobs")

		return nil
	}
	listOpts := metav1.ListOptions{LabelSelector: fmt.Sprintf("%s=%s,%s", k8sutil.AppAttr, AppName, OSDOverPVCLabelKey)}

	deployments, err := c.context.Clientset.AppsV1().Deployments(c.clusterInfo.Namespace).List(c.clusterInfo.Context, listOpts)
	if err != nil {
		return errors.Wrap(err, "failed to query existing OSD deployments")
	}
	logger.Infof("found deployments: %v", len(deployments.Items))

	cronJobs := make(map[string]bool, len(deployments.Items))
	for _, osdDep := range deployments.Items {
		osd, err := c.getOSDInfo(&osdDep)
		if err != nil {
			return errors.Wrap(err, "failed to get osd info")
		}
		pvcName, ok := osdDep.Labels[OSDOverPVCLabelKey]
		if !ok {
			return errors.Errorf("failed to get pvc name for osd %q", osdDep.Name)
		}
		osdProps, err := c.getOSDPropsForPVC(pvcName, osd.DeviceClass)
		if err != nil {
			return errors.Wrapf(err, "failed to generate config for osd %q", osdDep.Name)
		}
		if !osdProps.encrypted {
			continue
		}

		logger.Infof("starting OSD key rotation cron job for osd %q", osd.ID)
		cj, err := c.makeKeyRotationCronJob(osdDep.Labels[OSDOverPVCLabelKey], osd, osdProps)
		if err != nil {
			return errors.Wrap(err, "failed to make key rotation cron job")
		}

		err = ctrl.SetOwnerReference(&osdDep, cj, c.context.Client.Scheme())
		if err != nil {
			return errors.Wrapf(err, "failed to set controllerReference on cron job %q", cj.Name)
		}

		_, err = k8sutil.CreateOrUpdateCronJob(c.clusterInfo.Context, c.context.Clientset, cj)
		if err != nil {
			return errors.Wrapf(err, "failed to create or update key rotation cron job %q", cj.Name)
		}
		logger.Infof("started OSD key rotation cron job %q", cj.Name)
		cronJobs[cj.Name] = true
	}
	logger.Infof("successfully started OSD key rotation cron jobs")

	existingCronJobsList, err := c.context.Clientset.BatchV1().
		CronJobs(c.clusterInfo.Namespace).
		List(c.clusterInfo.Context, listOpts)
	if err != nil {
		return errors.Wrap(err, "failed to query existing key rotation cron jobs")
	}
	for _, cj := range existingCronJobsList.Items {
		// delete the cron job if it is not in the list created from the OSD deployments.
		if _, ok := cronJobs[cj.Name]; !ok {
			err := c.context.Clientset.BatchV1().CronJobs(c.clusterInfo.Namespace).Delete(c.clusterInfo.Context, cj.Name, metav1.DeleteOptions{})
			if client.IgnoreNotFound(err) != nil {
				return errors.Wrapf(err, "failed to delete key rotation cron job %q", cj.Name)
			}
			logger.Infof("successfully deleted key rotation cron job %q", cj.Name)
		}
	}

	return nil
}
