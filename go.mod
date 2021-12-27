module github.com/vmware-tanzu/astrolabe-velero

go 1.14

replace github.com/vmware-tanzu/astrolabe => ../astrolabe

replace github.com/vmware-tanzu/velero => ../velero

require (
	cloud.google.com/go v0.54.0 // indirect
	github.com/Azure/go-autorest/autorest v0.11.1 // indirect
	github.com/Azure/go-autorest/autorest/adal v0.9.5 // indirect
	github.com/google/uuid v1.1.2
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.7.0
	github.com/vmware-tanzu/astrolabe v0.0.0-00010101000000-000000000000
	github.com/vmware-tanzu/velero v0.0.0-00010101000000-000000000000
	k8s.io/api v0.19.7
	k8s.io/apimachinery v0.20.4
	k8s.io/client-go v0.19.7
	k8s.io/utils v0.0.0-20201110183641-67b214c5f920 // indirect
)
