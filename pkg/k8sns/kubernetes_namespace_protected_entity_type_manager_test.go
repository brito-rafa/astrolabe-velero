package k8sns

import (
	"context"
	"github.com/sirupsen/logrus"
	"github.com/vmware-tanzu/astrolabe/pkg/astrolabe"
	"strings"
	"testing"
)

func TestSnapshot(t *testing.T) {
	context := context.Background()
	params := map[string]interface{} {
		"kubeconfigPath" : "/home/dsmithuchida/.kube/config",
	}
	logger := logrus.New()
	k8sPETM, err := NewKubernetesNamespaceProtectedEntityTypeManagerFromConfig(params, astrolabe.S3Config{URLBase: "k8sns/"}, logger)
	if err != nil {
		t.Fatalf("Failed in NewKubernetesNamespaceProtectedEntityTypeManagerFromConfig with err %v\n", err)
	}
	namespacePEs, err := k8sPETM.GetProtectedEntities(context)
	if err != nil {
		t.Fatalf("GetProtectedEntites failed with %v", err)
	}
	for i, curPEID := range namespacePEs {
		t.Logf("Namespace %d: %s\n", i, curPEID.String())
		if strings.HasSuffix(curPEID.String(), "kibishii") {
			nsPE, err := k8sPETM.GetProtectedEntity(context, curPEID)
			if err != nil {
				t.Fatalf("GetProtectedEntity for ID %s failed with %v", curPEID.String(), err)
			}
			snapshotID, err := nsPE.Snapshot(context, make(map[string]map[string]interface{}, 0))
			if err != nil {
				t.Fatalf("Snapshot for ID %s failed with %v", curPEID.String(), err)
			}
			t.Logf("Took snapshot, ID = %s", snapshotID.String())
		}
	}
}
