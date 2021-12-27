/*
 * Copyright 2019 the Velero contributors
 * SPDX-License-Identifier: Apache-2.0
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package k8sns

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/vmware-tanzu/astrolabe/pkg/astrolabe"
	velerov1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/archive"
	"github.com/vmware-tanzu/velero/pkg/backup"
	"github.com/vmware-tanzu/velero/pkg/builder"
	"github.com/vmware-tanzu/velero/pkg/client"
	"github.com/vmware-tanzu/velero/pkg/discovery"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	"github.com/vmware-tanzu/velero/pkg/podexec"
	"github.com/vmware-tanzu/velero/pkg/util/filesystem"
	"io"
	"k8s.io/apimachinery/pkg/api/meta"
)

type KubernetesNamespaceProtectedEntity struct {
	petm      * KubernetesNamespaceProtectedEntityTypeManager
	id        astrolabe.ProtectedEntityID
	name      string
	logger    logrus.FieldLogger
	actions   []velero.BackupItemAction
}

func NewKubernetesNamespaceProtectedEntity(petm *KubernetesNamespaceProtectedEntityTypeManager, nsPEID astrolabe.ProtectedEntityID,
	name string, actions   []velero.BackupItemAction) (*KubernetesNamespaceProtectedEntity, error) {
	returnPE := KubernetesNamespaceProtectedEntity{
		petm:      petm,
		id:        nsPEID,
		logger:    petm.logger,
		name: name,
		actions: actions,
	}
	return &returnPE, nil
}


func (recv *KubernetesNamespaceProtectedEntity) GetDataReader(context.Context) (io.ReadCloser, error) {
	if !recv.id.HasSnapshot() {
		vc := client.VeleroConfig{}
		f := client.NewFactory("astrolabe", vc)

		veleroClient, err := f.Client()
		if err != nil {
			return nil, err
		}
		discoveryClient := veleroClient.Discovery()

		dynamicClient, err := f.DynamicClient()
		if err != nil {
			return nil, err
		}

		discoveryHelper, err := discovery.NewHelper(discoveryClient, recv.logger)
		if err != nil {
			return nil, err
		}
		dynamicFactory := client.NewDynamicFactory(dynamicClient)

		kubeClient, err := f.KubeClient()
		if err != nil {
			return nil, err
		}

		kubeClientConfig, err := f.ClientConfig()
		if err != nil {
			return nil, err
		}
		podCommandExecutor := podexec.NewPodCommandExecutor(kubeClientConfig, kubeClient.CoreV1().RESTClient())
		defaultVolumesToRestic := false
		k8sBackupper, err := backup.NewKubernetesBackupper(veleroClient.VeleroV1(),
			discoveryHelper,
			dynamicFactory,
			podCommandExecutor,
			nil,
			0,
			defaultVolumesToRestic)
		if err != nil {
			return nil, err
		}

		snapshotUUID, err := uuid.NewRandom()
		if err != nil {
			return nil, err
		}

		reader, writer := io.Pipe()
		backupParams := 	builder.ForBackup(velerov1.DefaultNamespace, "astrolabe-" + snapshotUUID.String()).
			IncludedNamespaces(recv.name).DefaultVolumesToRestic(false).Result()

		request := backup.Request{
			Backup:                    backupParams,
		}

		go recv.runBackup(k8sBackupper, request, writer)

		return reader, nil
	}
	return recv.petm.internalRepo.GetDataReaderForSnapshot(recv.id)
}

func (recv * KubernetesNamespaceProtectedEntity)runBackup(k8sBackupper backup.Backupper, request backup.Request, writer io.WriteCloser) {
	defer writer.Close()
	k8sBackupper.Backup(recv.logger, &request, writer, recv.actions, nil)
}

func (recv *KubernetesNamespaceProtectedEntity) GetMetadataReader(context.Context) (io.ReadCloser, error) {
	return nil, nil
}



func (recv *KubernetesNamespaceProtectedEntity) GetInfo(ctx context.Context) (astrolabe.ProtectedEntityInfo, error) {

	dataS3Transport, err := astrolabe.NewS3DataTransportForPEID(recv.id, recv.petm.s3Config)
	if err != nil {
		return nil, errors.Wrap(err, "Could not create S3 data transport")
	}

	data := []astrolabe.DataTransport{
		dataS3Transport,
	}

	mdS3Transport, err := astrolabe.NewS3MDTransportForPEID(recv.id, recv.petm.s3Config)
	if err != nil {
		return nil, errors.Wrap(err, "Could not create S3 md transport")
	}

	md := []astrolabe.DataTransport{
		mdS3Transport,
	}

	combinedS3Transport, err := astrolabe.NewS3CombinedTransportForPEID(recv.id, recv.petm.s3Config)
	if err != nil {
		return nil, errors.Wrap(err, "Could not create S3 combined transport")
	}

	combined := []astrolabe.DataTransport{
		combinedS3Transport,
	}

	components, err := recv.getComponentIDs(ctx)
	retVal := astrolabe.NewProtectedEntityInfo(
		recv.id,
		recv.name,
		-1,
		data,
		md,
		combined,
		components)
	return retVal, nil
}

func (recv *KubernetesNamespaceProtectedEntity) GetCombinedInfo(ctx context.Context) ([]astrolabe.ProtectedEntityInfo, error) {
	return nil, nil

}

func (recv *KubernetesNamespaceProtectedEntity) Snapshot(ctx context.Context, params map[string]map[string]interface{}) (astrolabe.ProtectedEntitySnapshotID, error) {
	if recv.id.HasSnapshot() {
		return astrolabe.ProtectedEntitySnapshotID{}, errors.New(fmt.Sprintf("pe %s is a snapshot, cannot snapshot again", recv.id.String()))
	}
	snapshotUUID, err := uuid.NewRandom()
	if err != nil {
		return astrolabe.ProtectedEntitySnapshotID{}, errors.Wrap(err, "Failed to create new UUID")
	}
	snapshotID := astrolabe.NewProtectedEntitySnapshotID(snapshotUUID.String())
	err = recv.petm.internalRepo.WriteProtectedEntity(ctx, recv, snapshotID)
	if err != nil {
		return astrolabe.ProtectedEntitySnapshotID{}, errors.Wrap(err, "Failed to create new snapshot")
	}
	return snapshotID, nil
}

func (recv *KubernetesNamespaceProtectedEntity) ListSnapshots(ctx context.Context) ([]astrolabe.ProtectedEntitySnapshotID, error) {
	return recv.petm.internalRepo.ListSnapshotsForPEID(recv.id)

}
func (recv *KubernetesNamespaceProtectedEntity) DeleteSnapshot(ctx context.Context, snapshotToDelete astrolabe.ProtectedEntitySnapshotID, params map[string]map[string]interface{}) (bool, error) {
	return false, nil

}
func (recv *KubernetesNamespaceProtectedEntity) GetInfoForSnapshot(ctx context.Context,
	snapshotID astrolabe.ProtectedEntitySnapshotID) (*astrolabe.ProtectedEntityInfo, error) {
	return nil, nil

}

func (recv *KubernetesNamespaceProtectedEntity) GetComponents(ctx context.Context) ([]astrolabe.ProtectedEntity, error) {
	return nil, nil
}

func (recv *KubernetesNamespaceProtectedEntity) getComponentIDs(ctx context.Context) ([]astrolabe.ProtectedEntityID, error) {
	returnComponents := []astrolabe.ProtectedEntityID{}
	if !recv.id.HasSnapshot() {
		// We should look for our components in the
		return returnComponents, nil
	}
	// get items out of backup tarball into a temp directory
	tarReader, err := recv.petm.internalRepo.GetDataReaderForSnapshot(recv.id)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not retrieve reader for snapshot data")
	}
	fs := filesystem.NewFileSystem()
	logger := logrus.New()
	dir, err := archive.NewExtractor(logger, fs).UnzipAndExtractBackup(tarReader)
	if err != nil {
		return nil, errors.Wrapf(err, "error extracting backup")

	}
	defer fs.RemoveAll(dir)

	backupResources, err := archive.NewParser(logger, fs).Parse(dir)
	for resourceTypeName, resourceItems := range backupResources {
		fmt.Println("Resource Type = %s\n", resourceTypeName)
		for namespace, resources := range resourceItems.ItemsByNamespace {
			fmt.Println("Namespace = %s", namespace)
			// Process individual items from the backup
			for _, item := range resources {
				itemPath := archive.GetItemFilePath(dir, resourceTypeName, namespace, item)

				// obj is the Unstructured item from the backup
				obj, err := archive.Unmarshal(fs, itemPath)
				if err == nil {
					accessor := meta.NewAccessor()
					annotations, err := accessor.Annotations(obj)
					if err == nil {
						if annotations != nil {
							componentSnapshotIDStr := annotations["vmware-tanzu.astrolabe.snapshotID"]
							if componentSnapshotIDStr != "" {
								componentSnapshotID, err := astrolabe.NewProtectedEntityIDFromString(componentSnapshotIDStr)
								if err == nil {
									returnComponents = append(returnComponents, componentSnapshotID)
								}
							}
						}
					}
				}
			}
		}
	}
	return returnComponents, nil

}

func (recv *KubernetesNamespaceProtectedEntity) GetID() astrolabe.ProtectedEntityID {
	return recv.id
}

func (recv *KubernetesNamespaceProtectedEntity) Overwrite(ctx context.Context, sourcePE astrolabe.ProtectedEntity, params map[string]map[string]interface{},
	overwriteComponents bool) error {
	return nil
}
