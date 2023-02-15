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
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/pkg/errors"
	"github.com/rook/rook/cmd/rook/rook"
	"github.com/rook/rook/pkg/clusterd"
	"github.com/rook/rook/pkg/daemon/ceph/client"
	"github.com/rook/rook/pkg/daemon/ceph/osd"
	"github.com/rook/rook/pkg/daemon/ceph/osd/kms"
	clusterOSD "github.com/rook/rook/pkg/operator/ceph/cluster/osd"

	operator "github.com/rook/rook/pkg/operator/ceph"
	"github.com/rook/rook/pkg/operator/k8sutil"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	slotOne string = "1"
	slotTwo string = "2"
)

// KeyManagementCmd defines a top-level utility command which interacts with encrypted keys stored in
// Key Management Service (KMS).
var KeyManagementCmd = &cobra.Command{
	Use:   "key-management",
	Short: "key-management interacts with a given Key Management System and perform actions.",
	Long: `The secret sub-command helps interacting with Key Management System.
	It can perform various actions such as retrieving the content of a Key Encryption Key.`,
}

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
		Use:   "activate-key-rotation [kms-secret-key] [data-device] [metadata-device] [wal-device]",
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
	fmt.Println("Waiting for 30 seconds")
	time.Sleep(time.Second * 30)
	// osdID, ok := os.LookupEnv("ROOK_OSD_ID")
	// if !ok {
	// 	rook.TerminateFatal(errors.New("failed to find osd id"))
	// }
	// depName := clusterOSD.DeploymentName(osdID)
	// dep, err := context.Clientset.AppsV1().Deployments(keyManagementService.ClusterInfo.Namespace).Get(ctx, depName, metav1.GetOptions{})
	// if err != nil {
	// 	rook.TerminateFatal(errors.Wrapf(err, "failed to get deployment %q", depName))
	// }

	// dep.GetCreationTimestamp()

	fmt.Println("Fetching the secret")
	// Fetch the secret
	// keys in slot : K1
	// key in KMS: K1
	currentKey, err := keyManagementService.GetSecret(secretName)
	if err != nil {
		rook.TerminateFatal(errors.Wrapf(err, "failed to get secret %q", secretName))
	}

	fmt.Printf("Adding the secret %q to the device", currentKey)
	// Add currentKey to slot 2
	// keys in slot : K1 K1
	// key in KMS: K1
	for _, devicePath := range devicePaths {
		err = osd.AddEncryptionKey(context, devicePath, currentKey, currentKey, slotTwo)
		if err != nil {
			rook.TerminateFatal(errors.Wrapf(err, "failed to get secret %q", secretName))
		}
	}

	fmt.Println("Generating new secret")
	// Generate new key
	newKey, err := clusterOSD.GenerateDmCryptKey()
	if err != nil {
		rook.TerminateFatal(errors.Wrapf(err, "failed to generate new key"))
	}

	fmt.Printf("Adding the secret %q to the device", newKey)
	// Add newKey to slot 1
	// keys in slot : K2 K1
	// key in KMS: K1
	for _, devicePath := range devicePaths {
		err = osd.AddEncryptionKey(context, devicePath, currentKey, newKey, slotOne)
		if err != nil {
			rook.TerminateFatal(errors.Wrapf(err, "failed to get secret %q", secretName))
		}
	}

	fmt.Println("Updating the secret in the KMS")
	// Update new key in the KMS
	// keys in slot : K2 K1
	// key in KMS: K2
	err = keyManagementService.UpdateSecret(secretName, newKey)
	if err != nil {
		rook.TerminateFatal(errors.Wrapf(err, "failed to get secret %q", secretName))
	}

	fmt.Println("Fetching the secret from the KMS")
	// Fetch key to verify its the new key.
	// keys in slot : K2 K1
	// key in KMS: K2
	keyInKMS, err := keyManagementService.GetSecret(secretName)
	if keyInKMS != newKey {
		rook.TerminateFatal(errors.Wrapf(err, "failed to get secret %q", secretName))
	}

	fmt.Println("Removing the old key from the device")
	// Remove old key from slot 2.
	// keys in slot : K2
	// key in KMS: K2
	for _, devicePath := range devicePaths {
		err = osd.RemoveEncryptionKeySlot(context, devicePath, newKey, slotTwo)
		if err != nil {
			rook.TerminateFatal(errors.Wrapf(err, "failed to get secret %q", secretName))
		}
	}

	fmt.Println("Success")
}
