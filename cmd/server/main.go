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

package main

import (
	"github.com/vmware-tanzu/astrolabe-velero/pkg/k8sns"
	"github.com/vmware-tanzu/astrolabe/pkg/psql"
	"github.com/vmware-tanzu/astrolabe/pkg/server"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	"log"
)

func main() {
	addonInitFuncs := make(map[string]server.InitFunc)
	addonInitFuncs["k8sns"] = k8sns.NewKubernetesNamespaceProtectedEntityTypeManagerFromConfig
	addonInitFuncs["psql"] = psql.NewPSQLProtectedEntityTypeManager
	server, pem, err := server.ServerInit(addonInitFuncs)
	if err != nil {
		log.Fatalln("Error initializing server = %v\n", err)
	}
	petm := pem.GetProtectedEntityTypeManager(k8sns.Typename)
	if petm == nil {
		log.Fatalln("Could not get k8ns ProtectedEntityTypeManager")
	}
	k8snsPetm, ok := petm.(*k8sns.KubernetesNamespaceProtectedEntityTypeManager)
	if !ok {
		log.Fatalln("k8sns PETM returned is not a k8sns.KubernetesNamespaceProtectedEntityTypeManager")
	}
	astrolabeBackupAction, err := k8sns.NewAstrolabeBackupItemAction(pem)
	if err != nil {
		log.Fatalln("Error initializing AstrolabeBackupItemAction %v\n", err)
	}
	actions := []velero.BackupItemAction{
		astrolabeBackupAction,
	}
	k8snsPetm.SetActions(actions)
	defer server.Shutdown()

	// serve API
	if err := server.Serve(); err != nil {
		log.Fatalln(err)
	}
}