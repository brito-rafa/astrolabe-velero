package k8sns

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/vmware-tanzu/astrolabe/pkg/astrolabe"
	"github.com/vmware-tanzu/astrolabe/pkg/psql"
	"github.com/vmware-tanzu/velero-plugin-for-vsphere/pkg/plugin/util"
	v1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

type AstrolabeBackupItemAction struct{
	pem astrolabe.ProtectedEntityManager
	k8sPEMap map[string]string
}

func NewAstrolabeBackupItemAction(pem astrolabe.ProtectedEntityManager) (AstrolabeBackupItemAction, error){
	k8sPEMap := map[string]string {
//		"persistentvolumeclaims": astrolabe.PvcPEType,
		"postgresqls.acid.zalan.do": psql.Typename,
	}
	return AstrolabeBackupItemAction{
		pem: pem,
		k8sPEMap: k8sPEMap,
	}, nil
}

func (recv AstrolabeBackupItemAction) AppliesTo() (velero.ResourceSelector, error) {
	resources := make([]string, 0, len(recv.k8sPEMap))
	for resource := range recv.k8sPEMap {
		resources = append(resources, resource)
	}
	return velero.ResourceSelector{
		IncludedResources: resources,
	}, nil
}

func (recv AstrolabeBackupItemAction) Execute(item runtime.Unstructured, backup *v1.Backup) (runtime.Unstructured, []velero.ResourceIdentifier, error) {
	ctx := context.Background()
	accessor := meta.NewAccessor()

	selfLink, err := accessor.SelfLink(item)
	if err != nil {
		return nil, nil, errors.Wrap(err, "Failed to retrieve SelfLink")
	}
	crdName := util.SelfLinkToCRDName(selfLink)
	peType, found := recv.k8sPEMap[crdName]
	if !found {

	}

	peName, err := accessor.UID(item)
	if err != nil {
		return nil, nil, errors.Wrap(err, "Failed to retrieve UID")
	}
	peID := astrolabe.NewProtectedEntityID(peType, string(peName))
	pe, err := recv.pem.GetProtectedEntity(ctx, peID)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "Could not retrieve PE")
	}
	snapshotID, err := pe.Snapshot(ctx, map[string]map[string]interface{}{})
	if err != nil {
		return nil, nil, errors.Wrapf(err, "Could not snapshot PE")
	}
	fmt.Printf("snapshotID = %v\n", snapshotID)
	annotations, err := accessor.Annotations(item)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "Could not retrieve annotations")
	}
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations["vmware-tanzu.astrolabe.snapshotID"] = pe.GetID().IDWithSnapshot(snapshotID).String()
	err = accessor.SetAnnotations(item, annotations)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "Could not set annotations")
	}
	return item, nil, nil
}


// AddAnnotations adds the supplied key-values to the annotations on the object
func AddAnnotations(o *metav1.ObjectMeta, vals map[string]string) {
	if o.Annotations == nil {
		o.Annotations = make(map[string]string)
	}
	for k, v := range vals {
		o.Annotations[k] = v
	}
}