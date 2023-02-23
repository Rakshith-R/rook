/*
Copyright 2021 The Rook Authors. All rights reserved.

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

package main

import (
	"context"
	"os"
	"os/signal"

	"github.com/coreos/pkg/capnslog"
	"github.com/pkg/errors"
	"github.com/rook/rook/cmd/rook/rook"
	"github.com/rook/rook/pkg/clusterd"
	"github.com/rook/rook/pkg/daemon/ceph/client"
	"github.com/rook/rook/pkg/daemon/ceph/osd"
	"github.com/rook/rook/pkg/daemon/ceph/osd/kms"
	oposd "github.com/rook/rook/pkg/operator/ceph/cluster/osd"

	operator "github.com/rook/rook/pkg/operator/ceph"
	"github.com/rook/rook/pkg/operator/k8sutil"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	slotZero string = "0"
	slotOne  string = "1"
)

var (
	logger = capnslog.NewPackageLogger("github.com/rook/rook", "key-management")
	// KeyManagementCmd defines a top-level utility command which interacts with encrypted keys stored in
	// Key Management Service (KMS).
	KeyManagementCmd = &cobra.Command{
		Use:   "key-management",
		Short: "key-management interacts with a given Key Management System and perform actions.",
		Long: `The secret sub-command helps interacting with Key Management System.
	It can perform various actions such as retrieving the content of a Key Encryption Key and
	rotating Key Encryption Key.`,
	}
)

func init() {
	KeyManagementCmd.AddCommand(
		cliGetSecret(),
		cliRotateSecret(),
	)
}

func startSecret() (*kms.Config, *clusterd.Context) {
	// Initialize the context
	ctx, cancel := signal.NotifyContext(context.Background(), operator.ShutdownSignals...)
	defer cancel()

	namespace := os.Getenv(k8sutil.PodNamespaceEnvVar)
	if namespace == "" {
		rook.TerminateFatal(errors.New("failed to find pod namespace"))
	}

	name := os.Getenv("ROOK_CLUSTER_NAME")
	if name == "" {
		rook.TerminateFatal(errors.New("failed to find cluster's name"))
	}

	clusterInfo := client.NewClusterInfo(namespace, name)
	clusterInfo.Context = ctx
	context := rook.NewContext()

	// Fetch the CephCluster for the KMS details
	cephCluster, err := context.RookClientset.CephV1().CephClusters(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		rook.TerminateFatal(errors.Wrapf(err, "failed to get ceph cluster in namespace %q", namespace))
	}

	return kms.NewConfig(context, &cephCluster.Spec, clusterInfo), context
}

// cliGetSecret is the Cobra CLI call
func cliGetSecret() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get [kms-secret-key] [output-file]",
		Short: "Fetch a secret from a given KMS",
		Args:  cobra.ExactArgs(2),
		Run:   getSecret,
	}
	return cmd
}

func getSecret(cmd *cobra.Command, args []string) {
	// Initialize the context
	ctx, cancel := signal.NotifyContext(context.Background(), operator.ShutdownSignals...)
	defer cancel()

	secretName := args[0]
	secretPath := args[1]
	keyManagementService, _ := startSecret()
	keyManagementService.ClusterInfo.Context = ctx

	// Fetch the secret
	s, err := keyManagementService.GetSecret(secretName)
	if err != nil {
		rook.TerminateFatal(errors.Wrapf(err, "failed to get secret %q", secretName))
	}

	// Write down the secret to a file
	err = os.WriteFile(secretPath, []byte(s), 0400)
	if err != nil {
		rook.TerminateFatal(errors.Wrapf(err, "failed to write secret %q file to %q", secretName, secretPath))
	}
}

// cliRotateSecret is the Cobra CLI call
func cliRotateSecret() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rotate-key [kms-secret-key] [data-device] [metadata-device] [wal-device]",
		Short: "Rotate a secret from a given KMS",
		Args:  cobra.RangeArgs(2, 4),
		Run:   rotateSecret,
	}
	return cmd
}

func rotateSecret(cmd *cobra.Command, args []string) {
	// Initialize the context
	ctx, cancel := signal.NotifyContext(context.Background(), operator.ShutdownSignals...)
	defer cancel()
	secretName := args[0]
	devicePaths := args[1:]
	keyManagementService, context := startSecret()
	keyManagementService.ClusterInfo.Context = ctx

	logger.Debugf("fetching the current key")
	// Fetch the currentKey.
	currentKey, err := keyManagementService.GetSecret(secretName)
	if err != nil {
		rook.TerminateFatal(errors.Wrapf(err, "failed to get secret %q", secretName))
	}

	// Ensure currentKey is in slot 1.
	for _, devicePath := range devicePaths {
		logger.Debugf("adding the current key to slot %q of the device %q", slotOne, devicePath)
		err = osd.AddEncryptionKey(context, devicePath, currentKey, currentKey, slotOne)
		if err != nil {
			rook.TerminateFatal(errors.Wrapf(err, "failed to add the current key to slot %q of the device", slotOne))
		}
	}

	logger.Debugf("Generating new key")
	// Generate new key.
	newKey, err := oposd.GenerateDmCryptKey()
	if err != nil {
		rook.TerminateFatal(errors.Wrapf(err, "failed to generate new key"))
	}

	// Add newKey to slot 0.
	for _, devicePath := range devicePaths {
		logger.Debugf("removing key slot %q if filled of the device %q", slotZero, devicePath)
		err = osd.RemoveEncryptionKeySlot(context, devicePath, currentKey, slotZero)
		if err != nil {
			rook.TerminateFatal(err)
		}
		logger.Debugf("adding new key to slot %q of the device %q", slotZero, devicePath)
		err = osd.AddEncryptionKey(context, devicePath, currentKey, newKey, slotZero)
		if err != nil {
			rook.TerminateFatal(err)
		}
	}

	logger.Debugf("updating the new key in the KMS")
	// Update new key.
	err = keyManagementService.UpdateSecret(secretName, newKey)
	if err != nil {
		rook.TerminateFatal(err)
	}

	logger.Debugf("fetching the key from the KMS to verify it.")
	// Fetch key to verify its the new key.
	keyInKMS, err := keyManagementService.GetSecret(secretName)
	if keyInKMS != newKey {
		rook.TerminateFatal(err)
	}

	// Remove old key from slot 1.
	for _, devicePath := range devicePaths {
		logger.Debugf("removing the old key from the slot %q of the device %q", slotOne, devicePath)
		err = osd.RemoveEncryptionKeySlot(context, devicePath, newKey, slotOne)
		if err != nil {
			rook.TerminateFatal(err)
		}
	}

	logger.Debugf("Successfully rotated the key")
}
