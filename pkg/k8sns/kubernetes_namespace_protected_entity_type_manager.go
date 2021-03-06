/*
 * Copyright 2019 the Astrolabe contributors
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
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/vmware-tanzu/astrolabe/pkg/astrolabe"
	"github.com/vmware-tanzu/astrolabe/pkg/localsnap"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	v1 "k8s.io/api/core/v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

type KubernetesNamespaceProtectedEntityTypeManager struct {
	clientset  *kubernetes.Clientset
	logger     logrus.FieldLogger
	s3Config   astrolabe.S3Config
	internalRepo localsnap.LocalSnapshotRepo
	actions []velero.BackupItemAction
}
const 	SnapshotsDirKey = "snapshotsDir"
const Typename = "k8sns"

func NewKubernetesNamespaceProtectedEntityTypeManagerFromConfig(params map[string]interface{}, s3Config astrolabe.S3Config,
	logger logrus.FieldLogger) (astrolabe.ProtectedEntityTypeManager, error) {
	masterURLObj := params["masterURL"]
	masterURL := ""
	if masterURLObj != nil {
		masterURL = masterURLObj.(string)
	}

	snapshotsDir, hasSnapshotsDir := params[SnapshotsDirKey].(string)
	if !hasSnapshotsDir {
		return nil, errors.New("no " + SnapshotsDirKey + " param found")
	}

	localSnapshotRepo, err := localsnap.NewLocalSnapshotRepo(Typename, snapshotsDir)
	if err != nil {
		return nil, err
	}

	kubeconfgPathObj := params["kubeconfig"]
	kubeconfigPath := ""
	if kubeconfgPathObj != nil {
		kubeconfigPath = kubeconfgPathObj.(string)
	}
	config, err := clientcmd.BuildConfigFromFlags(masterURL, kubeconfigPath)
	if err != nil {
		return nil, err
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	returnTypeManager := KubernetesNamespaceProtectedEntityTypeManager{
		clientset: clientset,
		logger:    logger,
		s3Config:  s3Config,
		internalRepo: localSnapshotRepo,
	}
	return &returnTypeManager, nil
}


func (recv *KubernetesNamespaceProtectedEntityTypeManager) SetActions(actions []velero.BackupItemAction) {
	recv.actions = actions
}

func (recv KubernetesNamespaceProtectedEntityTypeManager) GetTypeName() string {
	return Typename
}

func (recv KubernetesNamespaceProtectedEntityTypeManager) GetProtectedEntity(ctx context.Context, id astrolabe.ProtectedEntityID) (
	astrolabe.ProtectedEntity, error) {
	if (id.HasSnapshot()) {
		peinfo, err := recv.internalRepo.GetPEInfoForID(ctx, id)
		if err != nil {
			return nil, errors.Wrap(err, "could not get peinfo")
		}
		return NewKubernetesNamespaceProtectedEntity(&recv, id, peinfo.GetName(), recv.actions)
	} else {
		namespace, err := recv.getNamespaceForPEID(ctx, id)
		if err != nil {
			return nil, errors.Wrap(err, "could not get namespace for id")
		}
		return NewKubernetesNamespaceProtectedEntity(&recv, id, namespace.Name, recv.actions)
	}
}

func (recv KubernetesNamespaceProtectedEntityTypeManager) getNamespaceForPEID(ctx context.Context, id astrolabe.ProtectedEntityID) (*v1.Namespace, error){
	namespaces, err := recv.clientset.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "Could not retrieve namespaces")
	}
	for _, curNamespace := range namespaces.Items {
		if string(curNamespace.UID) == id.GetID() {
			return &curNamespace, nil
		}
	}
	return nil, errors.New("Not found")
}
func (recv KubernetesNamespaceProtectedEntityTypeManager) GetProtectedEntities(ctx context.Context) ([]astrolabe.ProtectedEntityID, error) {
	namespaceList, err := recv.clientset.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})

	if err != nil {
		return []astrolabe.ProtectedEntityID{}, err
	}
	var returnList []astrolabe.ProtectedEntityID
	for _, namespace := range namespaceList.Items {
		returnList = append(returnList, astrolabe.NewProtectedEntityID(recv.GetTypeName(),
			string(namespace.UID)))
	}

	return returnList, nil
}

func (recv KubernetesNamespaceProtectedEntityTypeManager) Copy(ctx context.Context, pe astrolabe.ProtectedEntity, params map[string]map[string]interface{},
	options astrolabe.CopyCreateOptions) (astrolabe.ProtectedEntity, error) {
	return nil, nil
}

func (recv KubernetesNamespaceProtectedEntityTypeManager) CopyFromInfo(ctx context.Context, info astrolabe.ProtectedEntityInfo, params map[string]map[string]interface{},
	options astrolabe.CopyCreateOptions) (astrolabe.ProtectedEntity, error) {
	return nil, nil
}

func (recv KubernetesNamespaceProtectedEntityTypeManager) Delete(ctx context.Context, id astrolabe.ProtectedEntityID) error {
	return errors.New("Not implemented")
}