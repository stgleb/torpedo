package k8s

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	snap_v1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	ap_api "github.com/libopenstorage/autopilot-api/pkg/apis/autopilot/v1alpha1"
	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	k8s_ops "github.com/portworx/sched-ops/k8s"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/scheduler/spec"
	"github.com/portworx/torpedo/drivers/volume"
	"github.com/sirupsen/logrus"
	apps_api "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	storage_api "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes/scheme"
)

const (
	// SchedName is the name of the kubernetes scheduler driver implementation
	SchedName = "k8s"
	// SnapshotParent is the parameter key for the parent of a snapshot
	SnapshotParent = "snapshot_parent"
	k8sPodsRootDir = "/var/lib/kubelet/pods"
	// DeploymentSuffix is the suffix for deployment names stored as keys in maps
	DeploymentSuffix = "-dep"
	// StatefulSetSuffix is the suffix for statefulset names stored as keys in maps
	StatefulSetSuffix = "-ss"
	// SystemdSchedServiceName is the name of the system service responsible for scheduling
	// TODO Change this when running on openshift for the proper service name
	SystemdSchedServiceName = "kubelet"
	// ZoneK8SNodeLabel is label describing zone of the k8s node
	ZoneK8SNodeLabel = "failure-domain.beta.kubernetes.io/zone"
	// RegionK8SNodeLabel is node label describing region of the k8s node
	RegionK8SNodeLabel = "failure-domain.beta.kubernetes.io/region"
)

const (
	statefulSetValidateTimeout = 20 * time.Minute
	k8sNodeReadyTimeout        = 5 * time.Minute
	volDirCleanupTimeout       = 5 * time.Minute
	k8sObjectCreateTimeout     = 2 * time.Minute
	k8sDestroyTimeout          = 2 * time.Minute
	//FindFilesOnWorkerTimeout timeout for find files on worker
	FindFilesOnWorkerTimeout = 1 * time.Minute
	deleteTasksWaitTimeout   = 3 * time.Minute
	//DefaultRetryInterval  Default retry interval
	DefaultRetryInterval = 10 * time.Second
	//DefaultTimeout default timeout
	DefaultTimeout               = 2 * time.Minute
	resizeSupportedAnnotationKey = "torpedo/resize-supported"
	autopilotResizeAnnotationKey = "torpedo.io/autopilot-enabled"
)

const (
	secretNameKey      = "secret_name"
	secretNamespaceKey = "secret_namespace"
	secretName         = "openstorage.io/auth-secret-name"
	secretNamespace    = "openstorage.io/auth-secret-namespace"
)

var (
	namespaceRegex = regexp.MustCompile("{{NAMESPACE}}")
)

//K8s  The kubernetes structure
type K8s struct {
	SpecFactory         *spec.Factory
	NodeDriverName      string
	VolDriverName       string
	secretConfigMapName string
}

//IsNodeReady  Check whether the cluster node is ready
func (k *K8s) IsNodeReady(n node.Node) error {
	t := func() (interface{}, bool, error) {
		if err := k8s_ops.Instance().IsNodeReady(n.Name); err != nil {
			return "", true, &scheduler.ErrNodeNotReady{
				Node:  n,
				Cause: err.Error(),
			}
		}

		return "", false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, k8sNodeReadyTimeout, DefaultRetryInterval); err != nil {
		return err
	}

	return nil
}

// String returns the string name of this driver.
func (k *K8s) String() string {
	return SchedName
}

//Init Initialize the driver
func (k *K8s) Init(specDir, volDriverName, nodeDriverName, secretConfigMap string) error {
	nodes, err := k8s_ops.Instance().GetNodes()
	if err != nil {
		return err
	}

	for _, n := range nodes.Items {
		if err = k.addNewNode(n); err != nil {
			return err
		}
	}

	k.SpecFactory, err = spec.NewFactory(specDir, k)
	if err != nil {
		return err
	}

	k.NodeDriverName = nodeDriverName
	k.VolDriverName = volDriverName

	k.secretConfigMapName = secretConfigMap
	return nil
}

func (k *K8s) addNewNode(newNode v1.Node) error {
	n := k.parseK8SNode(newNode)
	if err := k.IsNodeReady(n); err != nil {
		return err
	}
	if err := node.AddNode(n); err != nil {
		return err
	}
	return nil
}

//RescanSpecs Rescan the application spec file
//
func (k *K8s) RescanSpecs(specDir string) error {
	var err error
	logrus.Infof("Rescanning specs for %v", specDir)
	k.SpecFactory, err = spec.NewFactory(specDir, k)
	if err != nil {
		return err
	}
	return nil
}

//RefreshNodeRegistry update the k8 node list registry
//
func (k *K8s) RefreshNodeRegistry() error {

	nodes, err := k8s_ops.Instance().GetNodes()
	if err != nil {
		return err
	}

	node.CleanupRegistry()

	for _, n := range nodes.Items {
		if err = k.addNewNode(n); err != nil {
			return err
		}
	}
	return nil
}

//ParseSpecs Parse the application spec file
//
func (k *K8s) ParseSpecs(specDir string) ([]interface{}, error) {
	fileList := []string{}
	if err := filepath.Walk(specDir, func(path string, f os.FileInfo, err error) error {
		if !f.IsDir() {
			fileList = append(fileList, path)
		}

		return nil
	}); err != nil {
		return nil, err
	}

	var specs []interface{}

	for _, fileName := range fileList {
		file, err := os.Open(fileName)
		if err != nil {
			return nil, err
		}
		defer file.Close()

		reader := bufio.NewReader(file)
		specReader := yaml.NewYAMLReader(reader)

		for {
			specContents, err := specReader.Read()
			if err == io.EOF {
				break
			}

			if len(bytes.TrimSpace(specContents)) > 0 {
				obj, err := decodeSpec(specContents)
				if err != nil {
					logrus.Warnf("Error decoding spec from %v: %v", fileName, err)
					return nil, err
				}

				specObj, err := validateSpec(obj)
				if err != nil {
					logrus.Warnf("Error parsing spec from %v: %v", fileName, err)
					return nil, err
				}

				specs = append(specs, specObj)
			}
		}
	}

	return specs, nil
}

func decodeSpec(specContents []byte) (runtime.Object, error) {
	obj, _, err := scheme.Codecs.UniversalDeserializer().Decode([]byte(specContents), nil, nil)
	if err != nil {
		scheme := runtime.NewScheme()
		if err := snap_v1.AddToScheme(scheme); err != nil {
			return nil, err
		}

		if err := stork_api.AddToScheme(scheme); err != nil {
			return nil, err
		}

		if err := ap_api.AddToScheme(scheme); err != nil {
			return nil, err
		}

		codecs := serializer.NewCodecFactory(scheme)
		obj, _, err = codecs.UniversalDeserializer().Decode([]byte(specContents), nil, nil)
		if err != nil {
			return nil, err
		}
	}
	return obj, nil
}

func validateSpec(in interface{}) (interface{}, error) {
	if specObj, ok := in.(*apps_api.Deployment); ok {
		return specObj, nil
	} else if specObj, ok := in.(*apps_api.StatefulSet); ok {
		return specObj, nil
	} else if specObj, ok := in.(*apps_api.DaemonSet); ok {
		return specObj, nil
	} else if specObj, ok := in.(*v1.Service); ok {
		return specObj, nil
	} else if specObj, ok := in.(*v1.PersistentVolumeClaim); ok {
		return specObj, nil
	} else if specObj, ok := in.(*storage_api.StorageClass); ok {
		return specObj, nil
	} else if specObj, ok := in.(*snap_v1.VolumeSnapshot); ok {
		return specObj, nil
	} else if specObj, ok := in.(*stork_api.GroupVolumeSnapshot); ok {
		return specObj, nil
	} else if specObj, ok := in.(*v1.Secret); ok {
		return specObj, nil
	} else if specObj, ok := in.(*v1.ConfigMap); ok {
		return specObj, nil
	} else if specObj, ok := in.(*stork_api.Rule); ok {
		return specObj, nil
	} else if specObj, ok := in.(*v1.Pod); ok {
		return specObj, nil
	} else if specObj, ok := in.(*stork_api.ClusterPair); ok {
		return specObj, nil
	} else if specObj, ok := in.(*stork_api.Migration); ok {
		return specObj, nil
	} else if specObj, ok := in.(*stork_api.MigrationSchedule); ok {
		return specObj, nil
	} else if specObj, ok := in.(*stork_api.BackupLocation); ok {
		return specObj, nil
	} else if specObj, ok := in.(*stork_api.ApplicationBackup); ok {
		return specObj, nil
	} else if specObj, ok := in.(*stork_api.SchedulePolicy); ok {
		return specObj, nil
	} else if specObj, ok := in.(*stork_api.ApplicationRestore); ok {
		return specObj, nil
	} else if specObj, ok := in.(*stork_api.ApplicationClone); ok {
		return specObj, nil
	} else if specObj, ok := in.(*stork_api.VolumeSnapshotRestore); ok {
		return specObj, nil
	} else if specObj, ok := in.(*ap_api.AutopilotRule); ok {
		return specObj, nil
	}

	return nil, fmt.Errorf("Unsupported object: %v", reflect.TypeOf(in))
}

//getAddressesForNode  Get IP address for the nodes in the cluster
//
func (k *K8s) getAddressesForNode(n v1.Node) []string {
	var addrs []string
	for _, addr := range n.Status.Addresses {
		if addr.Type == v1.NodeExternalIP || addr.Type == v1.NodeInternalIP {
			addrs = append(addrs, addr.Address)
		}
	}
	return addrs
}

//parseK8SNode Parse the kubernetes clsuter nodes
//
func (k *K8s) parseK8SNode(n v1.Node) node.Node {
	var nodeType node.Type
	var zone, region string
	if k8s_ops.Instance().IsNodeMaster(n) {
		nodeType = node.TypeMaster
	} else {
		nodeType = node.TypeWorker
	}

	nodeLabels, err := k8s_ops.Instance().GetLabelsOnNode(n.GetName())
	if err != nil {
		logrus.Warn("failed to get node label for ", n.GetName())
	}

	for key, value := range nodeLabels {
		switch key {
		case ZoneK8SNodeLabel:
			zone = value
		case RegionK8SNodeLabel:
			region = value
		}
	}

	return node.Node{
		Name:      n.Name,
		Addresses: k.getAddressesForNode(n),
		Type:      nodeType,
		Zone:      zone,
		Region:    region,
	}
}

//Schedule Schedule the application
func (k *K8s) Schedule(instanceID string, options scheduler.ScheduleOptions) ([]*scheduler.Context, error) {
	var apps []*spec.AppSpec
	if len(options.AppKeys) > 0 {
		for _, key := range options.AppKeys {
			spec, err := k.SpecFactory.Get(key)
			if err != nil {
				return nil, err
			}
			apps = append(apps, spec)
		}
	} else {
		apps = k.SpecFactory.GetAll()
	}

	var contexts []*scheduler.Context
	for _, app := range apps {

		appNamespace := app.GetID(instanceID)
		specObjects, err := k.CreateSpecObjects(app, appNamespace, options)
		if err != nil {
			return nil, err
		}

		ctx := &scheduler.Context{
			UID: instanceID,
			App: &spec.AppSpec{
				Key:      app.Key,
				SpecList: specObjects,
				Enabled:  app.Enabled,
			},
			Options: options,
		}

		contexts = append(contexts, ctx)
	}

	return contexts, nil
}

// CreateSpecObjects Create application
func (k *K8s) CreateSpecObjects(app *spec.AppSpec, namespace string, options scheduler.ScheduleOptions) ([]interface{}, error) {
	var specObjects []interface{}
	ns, err := k.createNamespace(app, namespace)
	if err != nil {
		return nil, err
	}

	for _, spec := range app.SpecList {
		t := func() (interface{}, bool, error) {
			obj, err := k.createMigrationObjects(spec, ns, app)
			if err != nil {
				return nil, true, err
			}
			return obj, false, nil
		}
		obj, err := task.DoRetryWithTimeout(t, k8sObjectCreateTimeout, DefaultRetryInterval)
		if err != nil {
			return nil, err
		}
		if obj != nil {
			specObjects = append(specObjects, obj)
		}
	}

	for _, spec := range app.SpecList {
		t := func() (interface{}, bool, error) {
			obj, err := k.createVolumeSnapshotRestore(spec, ns, app)
			if err != nil {
				return nil, true, err
			}
			return obj, false, nil
		}

		obj, err := task.DoRetryWithTimeout(t, k8sObjectCreateTimeout, DefaultRetryInterval)
		if err != nil {
			return nil, err
		}

		if obj != nil {
			specObjects = append(specObjects, obj)
		}
	}

	for _, spec := range app.SpecList {
		t := func() (interface{}, bool, error) {
			obj, err := k.createStorageObject(spec, ns, app, options)
			if err != nil {
				return nil, true, err
			}
			return obj, false, nil
		}

		obj, err := task.DoRetryWithTimeout(t, k8sObjectCreateTimeout, DefaultRetryInterval)
		if err != nil {
			return nil, err
		}

		if obj != nil {
			specObjects = append(specObjects, obj)
		}
	}

	for _, spec := range app.SpecList {
		t := func() (interface{}, bool, error) {
			obj, err := k.createCoreObject(spec, ns, app, options)
			if err != nil {
				return nil, true, err
			}
			return obj, false, nil
		}

		obj, err := task.DoRetryWithTimeout(t, k8sObjectCreateTimeout, DefaultRetryInterval)
		if err != nil {
			return nil, err
		}

		if obj != nil {
			specObjects = append(specObjects, obj)
		}
	}
	for _, spec := range app.SpecList {
		t := func() (interface{}, bool, error) {
			obj, err := k.createBackupObjects(spec, ns, app)
			if err != nil {
				return nil, true, err
			}
			return obj, false, nil
		}
		obj, err := task.DoRetryWithTimeout(t, k8sObjectCreateTimeout, DefaultRetryInterval)
		if err != nil {
			return nil, err
		}
		if obj != nil {
			specObjects = append(specObjects, obj)
		}
	}

	return specObjects, nil
}

// AddTasks adds tasks to an existing context
func (k *K8s) AddTasks(ctx *scheduler.Context, options scheduler.ScheduleOptions) error {
	if ctx == nil {
		return fmt.Errorf("Context to add tasks to cannot be nil")
	}
	if len(options.AppKeys) == 0 {
		return fmt.Errorf("Need to specify list of applications to add to context")
	}

	appNamespace := ctx.GetID()
	var apps []*spec.AppSpec
	specObjects := ctx.App.SpecList
	for _, key := range options.AppKeys {
		spec, err := k.SpecFactory.Get(key)
		if err != nil {
			return err
		}
		apps = append(apps, spec)
	}
	for _, app := range apps {
		objects, err := k.CreateSpecObjects(app, appNamespace, options)
		if err != nil {
			return err
		}
		specObjects = append(specObjects, objects...)
	}
	ctx.App.SpecList = specObjects
	return nil
}

// UpdateTasksID updates task IDs in the given context
func (k *K8s) UpdateTasksID(ctx *scheduler.Context, id string) error {
	ctx.UID = id

	for _, spec := range ctx.App.SpecList {
		metadata, err := meta.Accessor(spec)
		if err != nil {
			return err
		}
		metadata.SetNamespace(id)
	}
	return nil
}

func (k *K8s) createNamespace(app *spec.AppSpec, namespace string) (*v1.Namespace, error) {
	k8sOps := k8s_ops.Instance()

	t := func() (interface{}, bool, error) {
		ns, err := k8sOps.CreateNamespace(namespace,
			map[string]string{
				"creator": "torpedo",
				"app":     app.Key,
			})

		if errors.IsAlreadyExists(err) {
			if ns, err = k8sOps.GetNamespace(namespace); err == nil {
				return ns, false, nil
			}
		}

		if err != nil {
			return nil, true, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create namespace: %v. Err: %v", namespace, err),
			}
		}

		return ns, false, nil
	}

	nsObj, err := task.DoRetryWithTimeout(t, k8sObjectCreateTimeout, DefaultRetryInterval)
	if err != nil {
		return nil, err
	}

	return nsObj.(*v1.Namespace), nil
}

func (k *K8s) createStorageObject(spec interface{}, ns *v1.Namespace, app *spec.AppSpec, options scheduler.ScheduleOptions) (interface{}, error) {
	k8sOps := k8s_ops.Instance()

	// Add security annotations if running with auth-enabled
	configMapName := k.secretConfigMapName
	if configMapName != "" {
		configMap, err := k8sOps.GetConfigMap(configMapName, "default")
		if err != nil {
			return nil, &scheduler.ErrFailedToGetConfigMap{
				Name:  configMapName,
				Cause: fmt.Sprintf("Failed to get config map: Err: %v", err),
			}
		}

		err = k.addSecurityAnnotation(spec, configMap)
		if err != nil {
			return nil, fmt.Errorf("Failed to add annotations to storage object: %v", err)
		}

	}

	if obj, ok := spec.(*storage_api.StorageClass); ok {
		obj.Namespace = ns.Name
		logrus.Infof("Setting provisioner of %v to %v", obj.Name, volume.GetStorageProvisioner())
		obj.Provisioner = volume.GetStorageProvisioner()

		sc, err := k8sOps.CreateStorageClass(obj)
		if errors.IsAlreadyExists(err) {
			if sc, err = k8sOps.GetStorageClass(obj.Name); err == nil {
				logrus.Infof("[%v] Found existing storage class: %v", app.Key, sc.Name)
				return sc, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create storage class: %v. Err: %v", obj.Name, err),
			}
		}

		logrus.Infof("[%v] Created storage class: %v", app.Key, sc.Name)
		return sc, nil

	} else if obj, ok := spec.(*v1.PersistentVolumeClaim); ok {
		obj.Namespace = ns.Name
		k.substituteNamespaceInPVC(obj, ns.Name)

		pvc, err := k8sOps.CreatePersistentVolumeClaim(obj)
		if errors.IsAlreadyExists(err) {
			if pvc, err = k8sOps.GetPersistentVolumeClaim(obj.Name, obj.Namespace); err == nil {
				logrus.Infof("[%v] Found existing PVC: %v", app.Key, pvc.Name)
				return pvc, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create PVC: %v. Err: %v", obj.Name, err),
			}
		}

		logrus.Infof("[%v] Created PVC: %v", app.Key, pvc.Name)

		pvcAnnotationSupported := false
		apParams := options.AutopilotParameters

		if apParams != nil && apParams.Enabled {
			if pvcAnnotation, ok := pvc.Annotations[autopilotResizeAnnotationKey]; ok {
				pvcAnnotationSupported, _ = strconv.ParseBool(pvcAnnotation)
			}
			if pvcAnnotationSupported {
				apParams.AutopilotRuleParameters.MatchLabels = pvc.Labels
				apObject, err := k.createAutopilotObject(apParams)
				if err != nil {
					return nil, &scheduler.ErrFailedToScheduleApp{
						App:   app,
						Cause: fmt.Sprintf("Failed to create Autopilot object: %v. Err: %v", apParams.Name, err),
					}
				}
				apRule, err := k.createAutopilotRule(apObject)
				if err != nil {
					return nil, &scheduler.ErrFailedToScheduleApp{
						App:   app,
						Cause: fmt.Sprintf("Failed to create Autopilot rule: %v. Err: %v", apObject, err),
					}
				}
				logrus.Infof("[%v] Created Autopilot rule: %v", app.Key, apRule.Name)
			}
		}
		return pvc, nil

	} else if obj, ok := spec.(*snap_v1.VolumeSnapshot); ok {
		obj.Metadata.Namespace = ns.Name
		snap, err := k8sOps.CreateSnapshot(obj)
		if errors.IsAlreadyExists(err) {
			if snap, err = k8sOps.GetSnapshot(obj.Metadata.Name, obj.Metadata.Namespace); err == nil {
				logrus.Infof("[%v] Found existing snapshot: %v", app.Key, snap.Metadata.Name)
				return snap, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create Snapshot: %v. Err: %v", obj.Metadata.Name, err),
			}
		}

		logrus.Infof("[%v] Created Snapshot: %v", app.Key, snap.Metadata.Name)
		return snap, nil
	} else if obj, ok := spec.(*stork_api.GroupVolumeSnapshot); ok {
		obj.Namespace = ns.Name
		snap, err := k8sOps.CreateGroupSnapshot(obj)
		if errors.IsAlreadyExists(err) {
			if snap, err = k8sOps.GetGroupSnapshot(obj.Name, obj.Namespace); err == nil {
				logrus.Infof("[%v] Found existing group snapshot: %v", app.Key, snap.Name)
				return snap, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create group snapshot: %v. Err: %v", obj.Name, err),
			}
		}

		logrus.Infof("[%v] Created Group snapshot: %v", app.Key, snap.Name)
		return snap, nil
	}

	return nil, nil
}

func (k *K8s) substituteNamespaceInPVC(pvc *v1.PersistentVolumeClaim, ns string) {
	pvc.Name = namespaceRegex.ReplaceAllString(pvc.Name, ns)
	for k, v := range pvc.Annotations {
		pvc.Annotations[k] = namespaceRegex.ReplaceAllString(v, ns)
	}
}

func (k *K8s) createVolumeSnapshotRestore(specObj interface{},
	ns *v1.Namespace,
	app *spec.AppSpec,
) (interface{}, error) {

	k8sOps := k8s_ops.Instance()
	if obj, ok := specObj.(*stork_api.VolumeSnapshotRestore); ok {
		obj.Namespace = ns.Name
		snapRestore, err := k8sOps.CreateVolumeSnapshotRestore(obj)
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create VolumeSnapshotRestore: %v. Err: %v", obj.Name, err),
			}
		}
		logrus.Infof("[%v] Created VolumeSnapshotRestore: %v", app.Key, snapRestore.Name)
		return snapRestore, nil
	}

	return nil, nil
}

func (k *K8s) addSecurityAnnotation(spec interface{}, configMap *v1.ConfigMap) error {
	logrus.Infof("Config Map details:\n %v:", configMap.Data)
	if _, ok := configMap.Data[secretNameKey]; !ok {
		return fmt.Errorf("Failed to get secret name from config map")
	}
	if _, ok := configMap.Data[secretNamespaceKey]; !ok {
		return fmt.Errorf("Failed to get secret namespace from config map")
	}
	if obj, ok := spec.(*v1.PersistentVolumeClaim); ok {
		if obj.Annotations == nil {
			obj.Annotations = make(map[string]string)
		}
		obj.Annotations[secretName] = configMap.Data[secretNameKey]
		obj.Annotations[secretNamespace] = configMap.Data[secretNamespaceKey]
	} else if obj, ok := spec.(*snap_v1.VolumeSnapshot); ok {
		if obj.Metadata.Annotations == nil {
			obj.Metadata.Annotations = make(map[string]string)
		}
		obj.Metadata.Annotations[secretName] = configMap.Data[secretNameKey]
		obj.Metadata.Annotations[secretNamespace] = configMap.Data[secretNamespaceKey]
	} else if obj, ok := spec.(*apps_api.StatefulSet); ok {
		for _, claim := range obj.Spec.VolumeClaimTemplates {
			if claim.Annotations == nil {
				claim.Annotations = make(map[string]string)
			}
			claim.Annotations[secretName] = configMap.Data[secretNameKey]
			claim.Annotations[secretNamespace] = configMap.Data[secretNamespaceKey]
		}
	} else if obj, ok := spec.(*stork_api.ApplicationBackup); ok {
		if obj.Annotations == nil {
			obj.Annotations = make(map[string]string)
		}
		obj.Annotations[secretName] = configMap.Data[secretNameKey]
		obj.Annotations[secretNamespace] = configMap.Data[secretNamespaceKey]
	} else if obj, ok := spec.(*stork_api.ApplicationClone); ok {
		if obj.Annotations == nil {
			obj.Annotations = make(map[string]string)
		}
		obj.Annotations[secretName] = configMap.Data[secretNameKey]
		obj.Annotations[secretNamespace] = configMap.Data[secretNamespaceKey]
	} else if obj, ok := spec.(*stork_api.ApplicationRestore); ok {
		if obj.Annotations == nil {
			obj.Annotations = make(map[string]string)
		}
		obj.Annotations[secretName] = configMap.Data[secretNameKey]
		obj.Annotations[secretNamespace] = configMap.Data[secretNamespaceKey]
	} else if obj, ok := spec.(*stork_api.Migration); ok {
		if obj.Annotations == nil {
			obj.Annotations = make(map[string]string)
		}
		obj.Annotations[secretName] = configMap.Data[secretNameKey]
		obj.Annotations[secretNamespace] = configMap.Data[secretNamespaceKey]
	} else if obj, ok := spec.(*stork_api.VolumeSnapshotRestore); ok {
		if obj.Annotations == nil {
			obj.Annotations = make(map[string]string)
		}
		obj.Annotations[secretName] = configMap.Data[secretNameKey]
		obj.Annotations[secretNamespace] = configMap.Data[secretNamespaceKey]
	} else if obj, ok := spec.(*stork_api.GroupVolumeSnapshot); ok {
		if obj.Annotations == nil {
			obj.Annotations = make(map[string]string)
		}
		obj.Annotations[secretName] = configMap.Data[secretNameKey]
		obj.Annotations[secretNamespace] = configMap.Data[secretNamespaceKey]
	}
	return nil
}

func (k *K8s) createCoreObject(spec interface{}, ns *v1.Namespace, app *spec.AppSpec, options scheduler.ScheduleOptions) (interface{}, error) {
	k8sOps := k8s_ops.Instance()
	if obj, ok := spec.(*apps_api.Deployment); ok {
		obj.Namespace = ns.Name
		obj.Spec.Template.Spec.Volumes = k.substituteNamespaceInVolumes(obj.Spec.Template.Spec.Volumes, ns.Name)
		dep, err := k8sOps.CreateDeployment(obj)
		if errors.IsAlreadyExists(err) {
			if dep, err = k8sOps.GetDeployment(obj.Name, obj.Namespace); err == nil {
				logrus.Infof("[%v] Found existing deployment: %v", app.Key, dep.Name)
				return dep, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create Deployment: %v. Err: %v", obj.Name, err),
			}
		}

		logrus.Infof("[%v] Created deployment: %v", app.Key, dep.Name)
		return dep, nil

	} else if obj, ok := spec.(*apps_api.StatefulSet); ok {
		// Add security annotations if running with auth-enabled
		configMapName := k.secretConfigMapName
		if configMapName != "" {
			configMap, err := k8sOps.GetConfigMap(configMapName, "default")
			if err != nil {
				return nil, &scheduler.ErrFailedToGetConfigMap{
					Name:  configMapName,
					Cause: fmt.Sprintf("Failed to get config map: Err: %v", err),
				}
			}

			err = k.addSecurityAnnotation(obj, configMap)
			if err != nil {
				return nil, fmt.Errorf("Failed to add annotations to core object: %v", err)
			}
		}

		obj.Namespace = ns.Name
		obj.Spec.Template.Spec.Volumes = k.substituteNamespaceInVolumes(obj.Spec.Template.Spec.Volumes, ns.Name)
		ss, err := k8sOps.CreateStatefulSet(obj)
		if errors.IsAlreadyExists(err) {
			if ss, err = k8sOps.GetStatefulSet(obj.Name, obj.Namespace); err == nil {
				logrus.Infof("[%v] Found existing StatefulSet: %v", app.Key, ss.Name)
				return ss, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create StatefulSet: %v. Err: %v", obj.Name, err),
			}
		}

		logrus.Infof("[%v] Created StatefulSet: %v", app.Key, ss.Name)
		return ss, nil

	} else if obj, ok := spec.(*v1.Service); ok {
		obj.Namespace = ns.Name
		svc, err := k8sOps.CreateService(obj)
		if errors.IsAlreadyExists(err) {
			if svc, err = k8sOps.GetService(obj.Name, obj.Namespace); err == nil {
				logrus.Infof("[%v] Found existing Service: %v", app.Key, svc.Name)
				return svc, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create Service: %v. Err: %v", obj.Name, err),
			}
		}

		logrus.Infof("[%v] Created Service: %v", app.Key, svc.Name)
		return svc, nil

	} else if obj, ok := spec.(*v1.Secret); ok {
		obj.Namespace = ns.Name
		secret, err := k8sOps.CreateSecret(obj)
		if errors.IsAlreadyExists(err) {
			if secret, err = k8sOps.GetSecret(obj.Name, obj.Namespace); err == nil {
				logrus.Infof("[%v] Found existing Secret: %v", app.Key, secret.Name)
				return secret, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create Secret: %v. Err: %v", obj.Name, err),
			}
		}

		logrus.Infof("[%v] Created Secret: %v", app.Key, secret.Name)
		return secret, nil
	} else if obj, ok := spec.(*stork_api.Rule); ok {
		if obj.Namespace != "kube-system" {
			obj.Namespace = ns.Name
		}
		rule, err := k8sOps.CreateRule(obj)
		if errors.IsAlreadyExists(err) {
			if rule, err = k8sOps.GetRule(obj.Name, obj.Namespace); err == nil {
				logrus.Infof("[%v] Found existing Rule: %v", app.Key, rule.GetName())
				return rule, nil
			}
		}

		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create Rule: %v, Err: %v", obj.Name, err),
			}
		}
		logrus.Infof("[%v] Created Rule: %v", app.Key, rule.GetName())
		return rule, nil
	} else if obj, ok := spec.(*v1.Pod); ok {
		obj.Namespace = ns.Name
		pod, err := k8sOps.CreatePod(obj)
		if errors.IsAlreadyExists(err) {
			if pod, err := k8sOps.GetPodByName(obj.Name, obj.Namespace); err == nil {
				logrus.Infof("[%v] Found existing Pods: %v", app.Key, pod.Name)
				return pod, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToSchedulePod{
				App:   app,
				Cause: fmt.Sprintf("Failed to create Pod: %v. Err: %v", obj.Name, err),
			}
		}

		logrus.Infof("[%v] Created Pod: %v", app.Key, pod.Name)
		return pod, nil
	} else if obj, ok := spec.(*v1.ConfigMap); ok {
		obj.Namespace = ns.Name
		configMap, err := k8sOps.CreateConfigMap(obj)
		if errors.IsAlreadyExists(err) {
			if configMap, err := k8sOps.GetConfigMap(obj.Name, obj.Namespace); err == nil {
				logrus.Infof("[%v] Found existing Config Maps: %v", app.Key, configMap.Name)
				return configMap, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create Config Map: %v. Err: %v", obj.Name, err),
			}
		}

		logrus.Infof("[%v] Created Config Map: %v", app.Key, configMap.Name)
		return configMap, nil
	}

	return nil, nil
}

func (k *K8s) destroyCoreObject(spec interface{}, opts map[string]bool, app *spec.AppSpec) (interface{}, error) {
	k8sOps := k8s_ops.Instance()
	var pods interface{}
	var podList []*v1.Pod
	var err error
	if obj, ok := spec.(*apps_api.Deployment); ok {
		if value, ok := opts[scheduler.OptionsWaitForResourceLeakCleanup]; ok && value {
			if pods, err = k8sOps.GetDeploymentPods(obj); err != nil {
				logrus.Warnf("[%s] Error getting deployment pods. Err: %v", app.Key, err)
			}
		}
		err := k8sOps.DeleteDeployment(obj.Name, obj.Namespace)
		if err != nil {
			return pods, &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to destroy Deployment: %v. Err: %v", obj.Name, err),
			}
		}
	} else if obj, ok := spec.(*apps_api.StatefulSet); ok {
		if value, ok := opts[scheduler.OptionsWaitForResourceLeakCleanup]; ok && value {
			if pods, err = k8sOps.GetStatefulSetPods(obj); err != nil {
				logrus.Warnf("[%v] Error getting statefulset pods. Err: %v", app.Key, err)
			}
		}
		err := k8sOps.DeleteStatefulSet(obj.Name, obj.Namespace)
		if err != nil {
			return pods, &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to destroy stateful set: %v. Err: %v", obj.Name, err),
			}
		}
	} else if obj, ok := spec.(*v1.Service); ok {
		err := k8sOps.DeleteService(obj.Name, obj.Namespace)
		if err != nil {
			return pods, &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to destroy Service: %v. Err: %v", obj.Name, err),
			}
		}

		logrus.Infof("[%v] Destroyed Service: %v", app.Key, obj.Name)
	} else if obj, ok := spec.(*stork_api.Rule); ok {
		err := k8sOps.DeleteRule(obj.Name, obj.Namespace)
		if err != nil {
			return pods, &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to destroy Rule: %v. Err: %v", obj.Name, err),
			}
		}

		logrus.Infof("[%v] Destroyed Rule: %v", app.Key, obj.Name)
	} else if obj, ok := spec.(*v1.Pod); ok {
		if value, ok := opts[scheduler.OptionsWaitForResourceLeakCleanup]; ok && value {
			pod, err := k8sOps.GetPodByName(obj.Name, obj.Namespace)
			if err != nil {
				logrus.Warnf("[%v] Error getting pods. Err: %v", app.Key, err)
			}
			podList = append(podList, pod)
			pods = podList
		}
		err := k8sOps.DeletePod(obj.Name, obj.Namespace, false)
		if err != nil {
			return pods, &scheduler.ErrFailedToDestroyPod{
				App:   app,
				Cause: fmt.Sprintf("Failed to destroy Pod: %v. Err: %v", obj.Name, err),
			}
		}

		logrus.Infof("[%v] Destroyed Pod: %v", app.Key, obj.Name)
	} else if obj, ok := spec.(*v1.ConfigMap); ok {
		if value, ok := opts[scheduler.OptionsWaitForResourceLeakCleanup]; ok && value {
			_, err := k8sOps.GetConfigMap(obj.Name, obj.Namespace)
			if err != nil {
				logrus.Warnf("[%v] Error getting config maps. Err: %v", app.Key, err)
			}
		}
		err := k8sOps.DeleteConfigMap(obj.Name, obj.Namespace)
		if err != nil {
			return pods, &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to destroy config map: %v. Err: %v", obj.Name, err),
			}
		}

		logrus.Infof("[%v] Destroyed Config Map: %v", app.Key, obj.Name)
	} else if obj, ok := spec.(*ap_api.AutopilotRule); ok {
		err := k8sOps.DeleteAutopilotRule(obj.Name)
		if err != nil {
			return pods, &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to destroy AutopilotRule: %v. Err: %v", obj.Name, err),
			}
		}

		logrus.Infof("[%v] Destroyed AutopilotRule: %v", app.Key, obj.Name)
	}

	return pods, nil

}

func (k *K8s) createAutopilotObject(apParams *scheduler.AutopilotParameters) (*ap_api.AutopilotRule, error) {
	obj := &ap_api.AutopilotRule{}
	apRuleParams := apParams.AutopilotRuleParameters
	obj.Name = apParams.Name
	obj.Namespace = apParams.Namespace
	obj.Labels = map[string]string{"creator": "torpedo"}
	obj.Spec.PollInterval = apParams.PollInterval
	obj.Spec.ActionsCoolDownPeriod = apRuleParams.ActionsCoolDownPeriod
	obj.Spec.Selector.LabelSelector.MatchLabels = apRuleParams.MatchLabels //map[string]string{"name": "pgbench-data"}
	obj.Spec.NamespaceSelector.LabelSelector.MatchLabels = map[string]string{"creator": "torpedo"}
	for _, ruleExpression := range apRuleParams.RuleConditionExpressions {
		exprVolumeUsage := &ap_api.LabelSelectorRequirement{
			Key:      ruleExpression.Key,
			Operator: ap_api.LabelSelectorOperator(ruleExpression.Operator),
			Values:   ruleExpression.Values,
		}
		obj.Spec.Conditions.Expressions = append(obj.Spec.Conditions.Expressions, exprVolumeUsage)
	}
	for _, ruleAction := range apRuleParams.RuleActions {
		actions := &ap_api.RuleAction{
			Name:   ruleAction.Name,
			Params: ruleAction.Params,
		}
		obj.Spec.Actions = append(obj.Spec.Actions, actions)
	}

	logrus.Infof("Using Autopilot Object: %+v\n", obj)

	return obj, nil
}

func (k *K8s) createAutopilotRule(autopilotRule *ap_api.AutopilotRule) (*ap_api.AutopilotRule, error) {
	k8sOps := k8s_ops.Instance()
	apRule, err := k8sOps.CreateAutopilotRule(autopilotRule)
	if errors.IsAlreadyExists(err) {
		if apRule, err := k8sOps.GetAutopilotRule(autopilotRule.Name); err == nil {
			logrus.Infof("Found existing AutopilotRule: %v", autopilotRule.Name)
			return apRule, nil
		}
	}
	if err != nil {
		return nil, fmt.Errorf("Failed to create AutopilotRule: %v. Err: %v", autopilotRule.Name, err)
	}

	return apRule, nil
}

func (k *K8s) substituteNamespaceInVolumes(volumes []v1.Volume, ns string) []v1.Volume {
	var updatedVolumes []v1.Volume
	for _, vol := range volumes {
		if vol.VolumeSource.PersistentVolumeClaim != nil {
			claimName := namespaceRegex.ReplaceAllString(vol.VolumeSource.PersistentVolumeClaim.ClaimName, ns)
			vol.VolumeSource.PersistentVolumeClaim.ClaimName = claimName
		}
		updatedVolumes = append(updatedVolumes, vol)
	}
	return updatedVolumes
}

//WaitForRunning   wait for running
//
func (k *K8s) WaitForRunning(ctx *scheduler.Context, timeout, retryInterval time.Duration) error {
	k8sOps := k8s_ops.Instance()
	for _, spec := range ctx.App.SpecList {
		if obj, ok := spec.(*apps_api.Deployment); ok {
			if err := k8sOps.ValidateDeployment(obj, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate Deployment: %v. Err: %v", obj.Name, err),
				}
			}

			logrus.Infof("[%v] Validated deployment: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := spec.(*apps_api.StatefulSet); ok {
			if err := k8sOps.ValidateStatefulSet(obj, timeout*time.Duration(*obj.Spec.Replicas)); err != nil {
				return &scheduler.ErrFailedToValidateApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate StatefulSet: %v. Err: %v", obj.Name, err),
				}
			}

			logrus.Infof("[%v] Validated statefulset: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := spec.(*v1.Service); ok {
			svc, err := k8sOps.GetService(obj.Name, obj.Namespace)
			if err != nil {
				return &scheduler.ErrFailedToValidateApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate Service: %v. Err: %v", obj.Name, err),
				}
			}

			logrus.Infof("[%v] Validated Service: %v", ctx.App.Key, svc.Name)
		} else if obj, ok := spec.(*stork_api.Rule); ok {
			svc, err := k8sOps.GetRule(obj.Name, obj.Namespace)
			if err != nil {
				return &scheduler.ErrFailedToValidateApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate Rule: %v. Err: %v", obj.Name, err),
				}
			}

			logrus.Infof("[%v] Validated Rule: %v", ctx.App.Key, svc.Name)
		} else if obj, ok := spec.(*v1.Pod); ok {
			if err := k8sOps.ValidatePod(obj, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidatePod{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate Pod: [%s] %s. Err: Pod is not ready %v", obj.Namespace, obj.Name, obj.Status),
				}
			}

			logrus.Infof("[%v] Validated pod: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := spec.(*stork_api.ClusterPair); ok {
			if err := k8sOps.ValidateClusterPair(obj.Name, obj.Namespace, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to validate cluster Pair: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}
			}
			logrus.Infof("[%v] Validated ClusterPair: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := spec.(*stork_api.Migration); ok {
			if err := k8sOps.ValidateMigration(obj.Name, obj.Namespace, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to validate Migration: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}
			}
			logrus.Infof("[%v] Validated Migration: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := spec.(*stork_api.MigrationSchedule); ok {
			if _, err := k8sOps.ValidateMigrationSchedule(obj.Name, obj.Namespace, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to validate MigrationSchedule: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}
			}
			logrus.Infof("[%v] Validated MigrationSchedule: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := spec.(*stork_api.BackupLocation); ok {
			if err := k8sOps.ValidateBackupLocation(obj.Name, obj.Namespace, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to validate BackupLocation: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}
			}
			logrus.Infof("[%v] Validated BackupLocation: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := spec.(*stork_api.ApplicationBackup); ok {
			if err := k8sOps.ValidateApplicationBackup(obj.Name, obj.Namespace, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to validate ApplicationBackup: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}
			}
			logrus.Infof("[%v] Validated ApplicationBackup: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := spec.(*stork_api.ApplicationRestore); ok {
			if err := k8sOps.ValidateApplicationRestore(obj.Name, obj.Namespace, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to validate ApplicationRestore: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}
			}
			logrus.Infof("[%v] Validated ApplicationRestore: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := spec.(*stork_api.ApplicationClone); ok {
			if err := k8sOps.ValidateApplicationClone(obj.Name, obj.Namespace, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to validate ApplicationClone: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}
			}
			logrus.Infof("[%v] Validated ApplicationClone: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := spec.(*stork_api.VolumeSnapshotRestore); ok {
			if err := k8sOps.ValidateVolumeSnapshotRestore(obj.Name, obj.Namespace, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to validate VolumeSnapshotRestore: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}
			}
			logrus.Infof("[%v] Validated VolumeSnapshotRestore: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := spec.(*snap_v1.VolumeSnapshot); ok {
			if err := k8sOps.ValidateSnapshot(obj.Metadata.Name, obj.Metadata.Namespace, true, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateCustomSpec{
					Name:  obj.Metadata.Name,
					Cause: fmt.Sprintf("Failed to validate VolumeSnapshotRestore: %v. Err: %v", obj.Metadata.Name, err),
					Type:  obj,
				}
			}
			logrus.Infof("[%v] Validated VolumeSnapshotRestore: %v", ctx.App.Key, obj.Metadata.Name)
		} else if obj, ok := spec.(*ap_api.AutopilotRule); ok {
			if _, err := k8sOps.GetAutopilotRule(obj.Name); err != nil {
				return &scheduler.ErrFailedToValidateCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to validate AutopilotRule: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}
			}
			logrus.Infof("[%v] Validated AutopilotRule: %v", ctx.App.Key, obj.Name)
		}
	}

	return nil
}

//Destroy destroy
func (k *K8s) Destroy(ctx *scheduler.Context, opts map[string]bool) error {
	var podList []v1.Pod
	for _, spec := range ctx.App.SpecList {
		t := func() (interface{}, bool, error) {
			currPods, err := k.destroyCoreObject(spec, opts, ctx.App)
			if err != nil {
				return nil, true, err
			}
			return currPods, false, nil
		}
		pods, err := task.DoRetryWithTimeout(t, k8sDestroyTimeout, DefaultRetryInterval)
		if err != nil {
			// in case we're not waiting for resource cleanup
			if value, ok := opts[scheduler.OptionsWaitForResourceLeakCleanup]; !ok || !value {
				return err
			}
			if pods != nil {
				podList = append(podList, pods.([]v1.Pod)...)
			}
			// we're ignoring this error since we want to verify cleanup down below, so simply logging it
			logrus.Warnf("Failed to destroy core objects. Cause: %v", err)
		}
	}
	for _, spec := range ctx.App.SpecList {
		t := func() (interface{}, bool, error) {
			err := k.destroyVolumeSnapshotRestoreObject(spec, ctx.App)
			if err != nil {
				return nil, true, err
			}
			return nil, false, nil
		}

		if _, err := task.DoRetryWithTimeout(t, k8sDestroyTimeout, DefaultRetryInterval); err != nil {
			return err
		}
	}
	for _, spec := range ctx.App.SpecList {
		t := func() (interface{}, bool, error) {
			err := k.destroyMigrationObject(spec, ctx.App)
			if err != nil {
				return nil, true, err
			}
			return nil, false, nil
		}

		if _, err := task.DoRetryWithTimeout(t, k8sDestroyTimeout, DefaultRetryInterval); err != nil {
			return err
		}
	}

	for _, spec := range ctx.App.SpecList {
		t := func() (interface{}, bool, error) {
			err := k.destroyBackupObjects(spec, ctx.App)
			if err != nil {
				return nil, true, err
			}
			return nil, false, nil
		}

		if _, err := task.DoRetryWithTimeout(t, k8sDestroyTimeout, DefaultRetryInterval); err != nil {
			return err
		}
	}

	if value, ok := opts[scheduler.OptionsWaitForResourceLeakCleanup]; ok && value {
		if err := k.WaitForDestroy(ctx, DefaultTimeout); err != nil {
			return err
		}
		if err := k.waitForCleanup(ctx, podList); err != nil {
			return err
		}
	} else if value, ok = opts[scheduler.OptionsWaitForDestroy]; ok && value {
		if err := k.WaitForDestroy(ctx, DefaultTimeout); err != nil {
			return err
		}
	}
	return nil
}

func (k *K8s) waitForCleanup(ctx *scheduler.Context, podList []v1.Pod) error {
	for _, pod := range podList {
		t := func() (interface{}, bool, error) {
			return nil, true, k.validateVolumeDirCleanup(pod.UID, ctx.App)
		}
		if _, err := task.DoRetryWithTimeout(t, volDirCleanupTimeout, DefaultRetryInterval); err != nil {
			return err
		}
		logrus.Infof("Validated resource cleanup for pod: %v", pod.UID)
	}
	return nil
}

func (k *K8s) validateVolumeDirCleanup(podUID types.UID, app *spec.AppSpec) error {
	podVolDir := k.getVolumeDirPath(podUID)
	driver, _ := node.Get(k.NodeDriverName)
	options := node.FindOpts{
		ConnectionOpts: node.ConnectionOpts{
			Timeout:         FindFilesOnWorkerTimeout,
			TimeBeforeRetry: DefaultRetryInterval,
		},
		MinDepth: 1,
		MaxDepth: 1,
	}

	for _, n := range node.GetWorkerNodes() {
		if volDir, err := driver.FindFiles(podVolDir, n, options); err != nil {
			return err
		} else if strings.TrimSpace(volDir) != "" {
			return &scheduler.ErrFailedToDeleteVolumeDirForPod{
				App:   app,
				Cause: fmt.Sprintf("Volume directory for pod %v still exists in node: %v", podUID, n.Name),
			}
		}
	}

	return nil
}

func (k *K8s) getVolumeDirPath(podUID types.UID) string {
	return filepath.Join(k8sPodsRootDir, string(podUID), "volumes")
}

//WaitForDestroy wait for schedule context destroy
//
func (k *K8s) WaitForDestroy(ctx *scheduler.Context, timeout time.Duration) error {
	k8sOps := k8s_ops.Instance()
	for _, spec := range ctx.App.SpecList {
		if obj, ok := spec.(*apps_api.Deployment); ok {
			if err := k8sOps.ValidateTerminatedDeployment(obj, timeout, DefaultRetryInterval); err != nil {
				return &scheduler.ErrFailedToValidateAppDestroy{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate destroy of deployment: %v. Err: %v", obj.Name, err),
				}
			}

			logrus.Infof("[%v] Validated destroy of Deployment: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := spec.(*apps_api.StatefulSet); ok {
			if err := k8sOps.ValidateTerminatedStatefulSet(obj, timeout, DefaultRetryInterval); err != nil {
				return &scheduler.ErrFailedToValidateAppDestroy{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate destroy of statefulset: %v. Err: %v", obj.Name, err),
				}
			}

			logrus.Infof("[%v] Validated destroy of StatefulSet: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := spec.(*v1.Service); ok {
			if err := k8sOps.ValidateDeletedService(obj.Name, obj.Namespace); err != nil {
				return &scheduler.ErrFailedToValidateAppDestroy{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate destroy of service: %v. Err: %v", obj.Name, err),
				}
			}

			logrus.Infof("[%v] Validated destroy of Service: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := spec.(*v1.Pod); ok {
			if err := k8sOps.WaitForPodDeletion(obj.UID, obj.Namespace, deleteTasksWaitTimeout); err != nil {
				return &scheduler.ErrFailedToValidatePodDestroy{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate destroy of pod: %v. Err: %v", obj.Name, err),
				}
			}

			logrus.Infof("[%v] Validated destroy of Pod: %v", ctx.App.Key, obj.Name)
		}
	}

	return nil
}

//DeleteTasks delete the task
func (k *K8s) DeleteTasks(ctx *scheduler.Context) error {
	k8sOps := k8s_ops.Instance()
	pods, err := k.getPodsForApp(ctx)
	if err != nil {
		return &scheduler.ErrFailedToDeleteTasks{
			App:   ctx.App,
			Cause: fmt.Sprintf("failed to get pods due to: %v", err),
		}
	}

	if err := k8sOps.DeletePods(pods, false); err != nil {
		return &scheduler.ErrFailedToDeleteTasks{
			App:   ctx.App,
			Cause: fmt.Sprintf("failed to delete pods due to: %v", err),
		}
	}

	// Ensure the pods are deleted and removed from the system
	for _, pod := range pods {
		err = k8sOps.WaitForPodDeletion(pod.UID, pod.Namespace, deleteTasksWaitTimeout)
		if err != nil {
			logrus.Errorf("k8s DeleteTasks failed to wait for pod: [%s] %s to terminate. err: %v", pod.Namespace, pod.Name, err)
			return err
		}
	}

	return nil
}

//GetVolumeParameters Get the volume parameters
func (k *K8s) GetVolumeParameters(ctx *scheduler.Context) (map[string]map[string]string, error) {
	k8sOps := k8s_ops.Instance()
	result := make(map[string]map[string]string)

	for _, spec := range ctx.App.SpecList {
		if obj, ok := spec.(*v1.PersistentVolumeClaim); ok {
			params, err := k8sOps.GetPersistentVolumeClaimParams(obj)
			if err != nil {
				return nil, &scheduler.ErrFailedToGetVolumeParameters{
					App:   ctx.App,
					Cause: fmt.Sprintf("failed to get params for volume: %v. Err: %v", obj.Name, err),
				}
			}

			pvc, err := k8sOps.GetPersistentVolumeClaim(obj.Name, obj.Namespace)
			if err != nil {
				return nil, &scheduler.ErrFailedToGetVolumeParameters{
					App:   ctx.App,
					Cause: fmt.Sprintf("failed to get PVC: %v. Err: %v", obj.Name, err),
				}
			}

			for k, v := range pvc.Annotations {
				params[k] = v
			}

			result[pvc.Spec.VolumeName] = params
		} else if obj, ok := spec.(*snap_v1.VolumeSnapshot); ok {
			snap, err := k8sOps.GetSnapshot(obj.Metadata.Name, obj.Metadata.Namespace)
			if err != nil {
				return nil, &scheduler.ErrFailedToGetVolumeParameters{
					App:   ctx.App,
					Cause: fmt.Sprintf("failed to get Snapshot: %v. Err: %v", obj.Metadata.Name, err),
				}
			}

			snapDataName := snap.Spec.SnapshotDataName
			if len(snapDataName) == 0 {
				return nil, &scheduler.ErrFailedToGetVolumeParameters{
					App:   ctx.App,
					Cause: fmt.Sprintf("snapshot: [%s] %s does not have snapshotdata set", snap.Metadata.Namespace, snap.Metadata.Name),
				}
			}

			snapData, err := k8sOps.GetSnapshotData(snapDataName)
			if err != nil {
				return nil, &scheduler.ErrFailedToGetVolumeParameters{
					App:   ctx.App,
					Cause: fmt.Sprintf("failed to get volumesnapshotdata: %s due to: %v", snapDataName, err),
				}
			}

			if snapData.Spec.VolumeSnapshotDataSource.PortworxSnapshot == nil ||
				len(snapData.Spec.VolumeSnapshotDataSource.PortworxSnapshot.SnapshotID) == 0 {
				return nil, &scheduler.ErrFailedToGetVolumeParameters{
					App:   ctx.App,
					Cause: fmt.Sprintf("volumesnapshotdata: %s does not have portworx volume source set", snapDataName),
				}
			}

			result[snapData.Spec.VolumeSnapshotDataSource.PortworxSnapshot.SnapshotID] = map[string]string{
				SnapshotParent: snap.Spec.PersistentVolumeClaimName,
			}
		} else if obj, ok := spec.(*apps_api.StatefulSet); ok {
			ss, err := k8sOps.GetStatefulSet(obj.Name, obj.Namespace)
			if err != nil {
				return nil, &scheduler.ErrFailedToGetVolumeParameters{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get StatefulSet: %v. Err: %v", obj.Name, err),
				}
			}

			pvcList, err := k8sOps.GetPVCsForStatefulSet(ss)
			if err != nil || pvcList == nil {
				return nil, &scheduler.ErrFailedToGetVolumeParameters{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get PVCs for StatefulSet: %v. Err: %v", ss.Name, err),
				}
			}

			for _, pvc := range pvcList.Items {
				params, err := k8sOps.GetPersistentVolumeClaimParams(&pvc)
				if err != nil {
					return nil, &scheduler.ErrFailedToGetVolumeParameters{
						App:   ctx.App,
						Cause: fmt.Sprintf("failed to get params for volume: %v. Err: %v", pvc.Name, err),
					}
				}

				for k, v := range pvc.Annotations {
					params[k] = v
				}

				result[pvc.Spec.VolumeName] = params
			}
		}
	}

	return result, nil
}

//InspectVolumes Insepect the volumes
func (k *K8s) InspectVolumes(ctx *scheduler.Context, timeout, retryInterval time.Duration) error {
	k8sOps := k8s_ops.Instance()
	for _, spec := range ctx.App.SpecList {
		if obj, ok := spec.(*storage_api.StorageClass); ok {
			if _, err := k8sOps.GetStorageClass(obj.Name); err != nil {
				return &scheduler.ErrFailedToValidateStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate StorageClass: %v. Err: %v", obj.Name, err),
				}
			}

			logrus.Infof("[%v] Validated storage class: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := spec.(*v1.PersistentVolumeClaim); ok {
			if err := k8sOps.ValidatePersistentVolumeClaim(obj, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate PVC: %v. Err: %v", obj.Name, err),
				}
			}
			logrus.Infof("[%v] Validated PVC: %v", ctx.App.Key, obj.Name)

			apParams := ctx.Options.AutopilotParameters
			pvcAnnotationSupported := false
			if apParams != nil && apParams.Enabled {
				if pvcAnnotation, ok := obj.Annotations[autopilotResizeAnnotationKey]; ok {
					pvcAnnotationSupported, _ = strconv.ParseBool(pvcAnnotation)
				}
				if pvcAnnotationSupported {
					expectedPVCSize := apParams.AutopilotRuleParameters.ExpectedPVCSize

					logrus.Infof("[%v] expecting PVC size: %+v\n", ctx.App.Key, expectedPVCSize)
					if err := k8sOps.ValidatePersistentVolumeClaimSize(obj, expectedPVCSize, timeout, retryInterval); err != nil {
						return &scheduler.ErrFailedToValidateStorage{
							App:   ctx.App,
							Cause: fmt.Sprintf("Failed to validate PVC %v of size: %v. Err: %v", obj.Name, expectedPVCSize, err),
						}
					}
				}
				logrus.Infof("[%v] Validated PVC: %v size based on Autopilot rule: %v calculation", ctx.App.Key, obj.Name, apParams.Name)
			}
		} else if obj, ok := spec.(*snap_v1.VolumeSnapshot); ok {
			if err := k8sOps.ValidateSnapshot(obj.Metadata.Name, obj.Metadata.Namespace, true, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate snapshot: %v. Err: %v", obj.Metadata.Name, err),
				}
			}

			logrus.Infof("[%v] Validated snapshot: %v", ctx.App.Key, obj.Metadata.Name)
		} else if obj, ok := spec.(*stork_api.GroupVolumeSnapshot); ok {
			if err := k8sOps.ValidateGroupSnapshot(obj.Name, obj.Namespace, true, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate group snapshot: %v. Err: %v", obj.Name, err),
				}
			}

			logrus.Infof("[%v] Validated group snapshot: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := spec.(*apps_api.StatefulSet); ok {
			ss, err := k8sOps.GetStatefulSet(obj.Name, obj.Namespace)
			if err != nil {
				return &scheduler.ErrFailedToValidateStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get StatefulSet: %v. Err: %v", obj.Name, err),
				}
			}

			if err := k8sOps.ValidatePVCsForStatefulSet(ss, timeout*time.Duration(*obj.Spec.Replicas), retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate PVCs for statefulset: %v. Err: %v", ss.Name, err),
				}
			}

			logrus.Infof("[%v] Validated PVCs from StatefulSet: %v", ctx.App.Key, obj.Name)
		}
	}

	return nil
}

func (k *K8s) isPVCShared(pvc *v1.PersistentVolumeClaim) bool {
	for _, mode := range pvc.Spec.AccessModes {
		if mode == v1.PersistentVolumeAccessMode(v1.ReadOnlyMany) ||
			mode == v1.PersistentVolumeAccessMode(v1.ReadWriteMany) {
			return true
		}
	}
	return false
}

//DeleteVolumes  delete the volumes
func (k *K8s) DeleteVolumes(ctx *scheduler.Context) ([]*volume.Volume, error) {
	k8sOps := k8s_ops.Instance()
	var vols []*volume.Volume
	for _, spec := range ctx.App.SpecList {
		if obj, ok := spec.(*storage_api.StorageClass); ok {
			if err := k8sOps.DeleteStorageClass(obj.Name); err != nil {
				if !errors.IsNotFound(err) {
					return nil, &scheduler.ErrFailedToDestroyStorage{
						App:   ctx.App,
						Cause: fmt.Sprintf("Failed to destroy storage class: %v. Err: %v", obj.Name, err),
					}
				}
			}

			logrus.Infof("[%v] Destroyed storage class: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := spec.(*v1.PersistentVolumeClaim); ok {
			pvcAnnotationSupported := false
			apParams := ctx.Options.AutopilotParameters

			pvc, err := k8sOps.GetPersistentVolumeClaim(obj.Name, obj.Namespace)
			if err != nil {
				return nil, &scheduler.ErrFailedToGetVolumeParameters{
					App:   ctx.App,
					Cause: fmt.Sprintf("failed to get PVC: %v. Err: %v", obj.Name, err),
				}
			}

			if apParams != nil && apParams.Enabled {
				if pvcAnnotation, ok := pvc.Annotations[autopilotResizeAnnotationKey]; ok {
					pvcAnnotationSupported, _ = strconv.ParseBool(pvcAnnotation)
				}
				if pvcAnnotationSupported {
					if err := k8sOps.DeleteAutopilotRule(apParams.Name); err != nil {
						if !errors.IsNotFound(err) {
							return nil, &scheduler.ErrFailedToDestroyAutopilotRule{
								Name:  apParams.Name,
								Cause: fmt.Sprintf("Failed to destroy an autopilot rule: %v. Err: %v", obj.Name, err),
							}
						}
					}
				}
			}
			vols = append(vols, &volume.Volume{
				ID:        string(obj.UID),
				Name:      obj.Name,
				Namespace: obj.Namespace,
				Shared:    k.isPVCShared(obj),
			})

			if err := k8sOps.DeletePersistentVolumeClaim(obj.Name, obj.Namespace); err != nil {
				if !errors.IsNotFound(err) {
					return nil, &scheduler.ErrFailedToDestroyStorage{
						App:   ctx.App,
						Cause: fmt.Sprintf("Failed to destroy PVC: %v. Err: %v", obj.Name, err),
					}
				}
			}

			logrus.Infof("[%v] Destroyed PVC: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := spec.(*snap_v1.VolumeSnapshot); ok {
			if err := k8sOps.DeleteSnapshot(obj.Metadata.Name, obj.Metadata.Namespace); err != nil {
				if !errors.IsNotFound(err) {
					return nil, &scheduler.ErrFailedToDestroyStorage{
						App:   ctx.App,
						Cause: fmt.Sprintf("Failed to destroy Snapshot: %v. Err: %v", obj.Metadata.Name, err),
					}
				}
			}

			logrus.Infof("[%v] Destroyed snapshot: %v", ctx.App.Key, obj.Metadata.Name)
		} else if obj, ok := spec.(*stork_api.GroupVolumeSnapshot); ok {
			if err := k8sOps.DeleteGroupSnapshot(obj.Name, obj.Namespace); err != nil {
				if !errors.IsNotFound(err) {
					return nil, &scheduler.ErrFailedToDestroyStorage{
						App:   ctx.App,
						Cause: fmt.Sprintf("Failed to destroy group snapshot: %v. Err: %v", obj.Name, err),
					}
				}
			}

			logrus.Infof("[%v] Destroyed group snapshot: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := spec.(*apps_api.StatefulSet); ok {
			pvcList, err := k8sOps.GetPVCsForStatefulSet(obj)
			if err != nil || pvcList == nil {
				return nil, &scheduler.ErrFailedToDestroyStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get PVCs for StatefulSet: %v. Err: %v", obj.Name, err),
				}
			}

			for _, pvc := range pvcList.Items {
				vols = append(vols, &volume.Volume{
					ID:        string(pvc.UID),
					Name:      pvc.Name,
					Namespace: pvc.Namespace,
					Shared:    k.isPVCShared(&pvc),
				})

				if err := k8sOps.DeletePersistentVolumeClaim(pvc.Name, pvc.Namespace); err != nil {
					if !errors.IsNotFound(err) {
						return nil, &scheduler.ErrFailedToDestroyStorage{
							App:   ctx.App,
							Cause: fmt.Sprintf("Failed to destroy PVC: %v. Err: %v", pvc.Name, err),
						}
					}
				}
			}

			logrus.Infof("[%v] Destroyed PVCs for StatefulSet: %v", ctx.App.Key, obj.Name)
		}
	}

	return vols, nil
}

//GetVolumes  Get the volumes
//
func (k *K8s) GetVolumes(ctx *scheduler.Context) ([]*volume.Volume, error) {
	k8sOps := k8s_ops.Instance()
	var vols []*volume.Volume
	for _, spec := range ctx.App.SpecList {
		if obj, ok := spec.(*v1.PersistentVolumeClaim); ok {
			vol := &volume.Volume{
				ID:        string(obj.UID),
				Name:      obj.Name,
				Namespace: obj.Namespace,
				Shared:    k.isPVCShared(obj),
			}
			vols = append(vols, vol)
		} else if obj, ok := spec.(*apps_api.StatefulSet); ok {
			ss, err := k8sOps.GetStatefulSet(obj.Name, obj.Namespace)
			if err != nil {
				return nil, &scheduler.ErrFailedToGetStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get StatefulSet: %v. Err: %v", obj.Name, err),
				}
			}

			pvcList, err := k8sOps.GetPVCsForStatefulSet(ss)
			if err != nil || pvcList == nil {
				return nil, &scheduler.ErrFailedToGetStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get PVC from StatefulSet: %v. Err: %v", ss.Name, err),
				}
			}

			for _, pvc := range pvcList.Items {
				vols = append(vols, &volume.Volume{
					ID:        string(pvc.UID),
					Name:      pvc.Name,
					Namespace: pvc.Namespace,
					Shared:    k.isPVCShared(&pvc),
				})
			}
		}
	}

	return vols, nil
}

//ResizeVolume  Resize the volume
func (k *K8s) ResizeVolume(ctx *scheduler.Context) ([]*volume.Volume, error) {
	k8sOps := k8s_ops.Instance()
	var vols []*volume.Volume
	for _, spec := range ctx.App.SpecList {
		if obj, ok := spec.(*v1.PersistentVolumeClaim); ok {
			updatedPVC, _ := k8sOps.GetPersistentVolumeClaim(obj.Name, obj.Namespace)
			vol, err := k.resizePVCBy1GB(ctx, updatedPVC)
			if err != nil {
				return nil, err
			}
			vols = append(vols, vol)
		} else if obj, ok := spec.(*apps_api.StatefulSet); ok {
			ss, err := k8sOps.GetStatefulSet(obj.Name, obj.Namespace)
			if err != nil {
				return nil, &scheduler.ErrFailedToResizeStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get StatefulSet: %v. Err: %v", obj.Name, err),
				}
			}

			pvcList, err := k8sOps.GetPVCsForStatefulSet(ss)
			if err != nil || pvcList == nil {
				return nil, &scheduler.ErrFailedToResizeStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get PVC from StatefulSet: %v. Err: %v", ss.Name, err),
				}
			}

			for _, pvc := range pvcList.Items {
				vol, err := k.resizePVCBy1GB(ctx, &pvc)
				if err != nil {
					return nil, err
				}
				vols = append(vols, vol)
			}
		}
	}

	return vols, nil
}

func (k *K8s) resizePVCBy1GB(ctx *scheduler.Context, pvc *v1.PersistentVolumeClaim) (*volume.Volume, error) {
	k8sOps := k8s_ops.Instance()
	storageSize := pvc.Spec.Resources.Requests[v1.ResourceStorage]

	// TODO this test is required since stork snapshot doesn't support resizing, remove when feature is added
	resizeSupported := true
	if annotationValue, hasKey := pvc.Annotations[resizeSupportedAnnotationKey]; hasKey {
		resizeSupported, _ = strconv.ParseBool(annotationValue)
	}
	if resizeSupported {
		extraAmount, _ := resource.ParseQuantity("1Gi")
		storageSize.Add(extraAmount)
		pvc.Spec.Resources.Requests[v1.ResourceStorage] = storageSize
		if _, err := k8sOps.UpdatePersistentVolumeClaim(pvc); err != nil {
			return nil, &scheduler.ErrFailedToResizeStorage{
				App:   ctx.App,
				Cause: err.Error(),
			}
		}
	}
	sizeInt64, _ := storageSize.AsInt64()
	vol := &volume.Volume{
		ID:        string(pvc.UID),
		Name:      pvc.Name,
		Namespace: pvc.Namespace,
		Size:      uint64(sizeInt64),
		Shared:    k.isPVCShared(pvc),
	}
	return vol, nil
}

//GetSnapshots  Get the snapshots
func (k *K8s) GetSnapshots(ctx *scheduler.Context) ([]*volume.Snapshot, error) {
	var snaps []*volume.Snapshot
	for _, spec := range ctx.App.SpecList {
		if obj, ok := spec.(*snap_v1.VolumeSnapshot); ok {
			snap := &volume.Snapshot{
				ID:        string(obj.Metadata.UID),
				Name:      obj.Metadata.Name,
				Namespace: obj.Metadata.Namespace,
			}
			snaps = append(snaps, snap)
		} else if obj, ok := spec.(*stork_api.GroupVolumeSnapshot); ok {
			snapsForGroupsnap, err := k8s_ops.Instance().GetSnapshotsForGroupSnapshot(obj.Name, obj.Namespace)
			if err != nil {
				return nil, err
			}

			for _, snapForGroupsnap := range snapsForGroupsnap {
				snap := &volume.Snapshot{
					ID:        string(snapForGroupsnap.Metadata.UID),
					Name:      snapForGroupsnap.Metadata.Name,
					Namespace: snapForGroupsnap.Metadata.Namespace,
				}
				snaps = append(snaps, snap)
			}
		}
	}

	return snaps, nil
}

//GetNodesForApp get the node for the app
//
func (k *K8s) GetNodesForApp(ctx *scheduler.Context) ([]node.Node, error) {
	t := func() (interface{}, bool, error) {
		pods, err := k.getPodsForApp(ctx)
		if err != nil {
			return nil, false, &scheduler.ErrFailedToGetNodesForApp{
				App:   ctx.App,
				Cause: fmt.Sprintf("failed to get pods due to: %v", err),
			}
		}

		// We should have pods from a supported application at this point
		var result []node.Node
		nodeMap := node.GetNodesByName()

		for _, p := range pods {
			if strings.TrimSpace(p.Spec.NodeName) == "" {
				return nil, true, &scheduler.ErrFailedToGetNodesForApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("pod %s is not scheduled to any node yet", p.Name),
				}
			}
			n, ok := nodeMap[p.Spec.NodeName]
			if !ok {
				return nil, true, &scheduler.ErrFailedToGetNodesForApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("node: %v not present in node map", p.Spec.NodeName),
				}
			}

			if node.Contains(result, n) {
				continue
			}

			if k8s_ops.Instance().IsPodRunning(p) {
				result = append(result, n)
			}
		}

		if len(result) > 0 {
			return result, false, nil
		}

		return result, true, &scheduler.ErrFailedToGetNodesForApp{
			App:   ctx.App,
			Cause: fmt.Sprintf("no pods in running state %v", pods),
		}
	}

	nodes, err := task.DoRetryWithTimeout(t, DefaultTimeout, DefaultRetryInterval)
	if err != nil {
		return nil, err
	}

	return nodes.([]node.Node), nil
}

func (k *K8s) getPodsForApp(ctx *scheduler.Context) ([]v1.Pod, error) {
	k8sOps := k8s_ops.Instance()
	var pods []v1.Pod

	for _, spec := range ctx.App.SpecList {
		if obj, ok := spec.(*apps_api.Deployment); ok {
			depPods, err := k8sOps.GetDeploymentPods(obj)
			if err != nil {
				return nil, err
			}
			pods = append(pods, depPods...)
		} else if obj, ok := spec.(*apps_api.StatefulSet); ok {
			ssPods, err := k8sOps.GetStatefulSetPods(obj)
			if err != nil {
				return nil, err
			}
			pods = append(pods, ssPods...)
		}
	}

	return pods, nil
}

//Describe describe the test case
func (k *K8s) Describe(ctx *scheduler.Context) (string, error) {
	k8sOps := k8s_ops.Instance()
	var buf bytes.Buffer
	var err error
	for _, spec := range ctx.App.SpecList {
		if obj, ok := spec.(*apps_api.Deployment); ok {
			buf.WriteString(insertLineBreak(obj.Name))
			var depStatus *apps_api.DeploymentStatus
			if depStatus, err = k8sOps.DescribeDeployment(obj.Name, obj.Namespace); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetAppStatus{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get status of deployment: %v. Err: %v", obj.Name, err),
				}))
			}
			//Dump depStatus
			buf.WriteString(fmt.Sprintf("%v\n", *depStatus))
			pods, _ := k8sOps.GetDeploymentPods(obj)
			for _, pod := range pods {
				buf.WriteString(dumpPodStatusRecursively(pod))
			}
			buf.WriteString(insertLineBreak("END Deployment"))
		} else if obj, ok := spec.(*apps_api.StatefulSet); ok {
			buf.WriteString(insertLineBreak(obj.Name))
			var ssetStatus *apps_api.StatefulSetStatus
			if ssetStatus, err = k8sOps.DescribeStatefulSet(obj.Name, obj.Namespace); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetAppStatus{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get status of statefulset: %v. Err: %v", obj.Name, err),
				}))
			}
			//Dump ssetStatus
			buf.WriteString(fmt.Sprintf("%v\n", *ssetStatus))
			pods, _ := k8sOps.GetStatefulSetPods(obj)
			for _, pod := range pods {
				buf.WriteString(dumpPodStatusRecursively(pod))
			}
			buf.WriteString(insertLineBreak("END StatefulSet"))
		} else if obj, ok := spec.(*v1.Service); ok {
			buf.WriteString(insertLineBreak(obj.Name))
			var svcStatus *v1.ServiceStatus
			if svcStatus, err = k8sOps.DescribeService(obj.Name, obj.Namespace); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetAppStatus{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get status of service: %v. Err: %v", obj.Name, err),
				}))
			}
			//Dump service status
			buf.WriteString(fmt.Sprintf("%v\n", *svcStatus))
			buf.WriteString(insertLineBreak("END Service"))
		} else if obj, ok := spec.(*v1.PersistentVolumeClaim); ok {
			buf.WriteString(insertLineBreak(obj.Name))
			var pvcStatus *v1.PersistentVolumeClaimStatus
			if pvcStatus, err = k8sOps.GetPersistentVolumeClaimStatus(obj); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetStorageStatus{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get status of persistent volume claim: %v. Err: %v", obj.Name, err),
				}))
			}
			//Dump persistent volume claim status
			buf.WriteString(fmt.Sprintf("%v\n", *pvcStatus))
			buf.WriteString(insertLineBreak("END PersistentVolumeClaim"))
		} else if obj, ok := spec.(*storage_api.StorageClass); ok {
			buf.WriteString(insertLineBreak(obj.Name))
			var scParams map[string]string
			if scParams, err = k8sOps.GetStorageClassParams(obj); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetVolumeParameters{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get parameters of storage class: %v. Err: %v", obj.Name, err),
				}))
			}
			//Dump storage class parameters
			buf.WriteString(fmt.Sprintf("%v\n", scParams))
			buf.WriteString(insertLineBreak("END Storage Class"))
		} else if obj, ok := spec.(*v1.Pod); ok {
			buf.WriteString(insertLineBreak(obj.Name))
			var podStatus *v1.PodList
			if podStatus, err = k8sOps.GetPods(obj.Name, nil); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetPodStatus{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get status of pod: %v. Err: %v", obj.Name, err),
				}))
			}
			buf.WriteString(fmt.Sprintf("%v\n", podStatus))
			buf.WriteString(insertLineBreak("END Pod"))
		} else {
			logrus.Warnf("Object type unknown/not supported: %v", obj)
		}
	}
	return buf.String(), nil
}

//ScaleApplication  Scale the application
func (k *K8s) ScaleApplication(ctx *scheduler.Context, scaleFactorMap map[string]int32) error {
	k8sOps := k8s_ops.Instance()
	for _, spec := range ctx.App.SpecList {
		if !k.IsScalable(spec) {
			continue
		}
		if obj, ok := spec.(*apps_api.Deployment); ok {
			logrus.Infof("Scale all Deployments")
			dep, err := k8sOps.GetDeployment(obj.Name, obj.Namespace)
			if err != nil {
				return err
			}
			newScaleFactor := scaleFactorMap[obj.Name+DeploymentSuffix]
			*dep.Spec.Replicas = newScaleFactor
			if _, err := k8sOps.UpdateDeployment(dep); err != nil {
				return &scheduler.ErrFailedToUpdateApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to update Deployment: %v. Err: %v", obj.Name, err),
				}
			}
			logrus.Infof("Deployment %s scaled to %d successfully.", obj.Name, newScaleFactor)
		} else if obj, ok := spec.(*apps_api.StatefulSet); ok {
			logrus.Infof("Scale all Stateful sets")
			ss, err := k8sOps.GetStatefulSet(obj.Name, obj.Namespace)
			if err != nil {
				return err
			}
			newScaleFactor := scaleFactorMap[obj.Name+StatefulSetSuffix]
			*ss.Spec.Replicas = newScaleFactor
			if _, err := k8sOps.UpdateStatefulSet(ss); err != nil {
				return &scheduler.ErrFailedToUpdateApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to update StatefulSet: %v. Err: %v", obj.Name, err),
				}
			}
			logrus.Infof("StatefulSet %s scaled to %d successfully.", obj.Name, int(newScaleFactor))
		}
	}
	return nil
}

//GetScaleFactorMap Get scale Factory map
//
func (k *K8s) GetScaleFactorMap(ctx *scheduler.Context) (map[string]int32, error) {
	k8sOps := k8s_ops.Instance()
	scaleFactorMap := make(map[string]int32, len(ctx.App.SpecList))
	for _, spec := range ctx.App.SpecList {
		if obj, ok := spec.(*apps_api.Deployment); ok {
			dep, err := k8sOps.GetDeployment(obj.Name, obj.Namespace)
			if err != nil {
				return scaleFactorMap, err
			}
			scaleFactorMap[obj.Name+DeploymentSuffix] = *dep.Spec.Replicas
		} else if obj, ok := spec.(*apps_api.StatefulSet); ok {
			ss, err := k8sOps.GetStatefulSet(obj.Name, obj.Namespace)
			if err != nil {
				return scaleFactorMap, err
			}
			scaleFactorMap[obj.Name+StatefulSetSuffix] = *ss.Spec.Replicas
		}
	}
	return scaleFactorMap, nil
}

//StopSchedOnNode stop schedule on node
func (k *K8s) StopSchedOnNode(n node.Node) error {
	driver, _ := node.Get(k.NodeDriverName)
	systemOpts := node.SystemctlOpts{
		ConnectionOpts: node.ConnectionOpts{
			Timeout:         FindFilesOnWorkerTimeout,
			TimeBeforeRetry: DefaultRetryInterval,
		},
		Action: "stop",
	}
	err := driver.Systemctl(n, SystemdSchedServiceName, systemOpts)
	if err != nil {
		return &scheduler.ErrFailedToStopSchedOnNode{
			Node:          n,
			SystemService: SystemdSchedServiceName,
			Cause:         err.Error(),
		}
	}
	return nil
}

//StartSchedOnNode start schedule on node
func (k *K8s) StartSchedOnNode(n node.Node) error {
	driver, _ := node.Get(k.NodeDriverName)
	systemOpts := node.SystemctlOpts{
		ConnectionOpts: node.ConnectionOpts{
			Timeout:         DefaultTimeout,
			TimeBeforeRetry: DefaultRetryInterval,
		},
		Action: "start",
	}
	err := driver.Systemctl(n, SystemdSchedServiceName, systemOpts)
	if err != nil {
		return &scheduler.ErrFailedToStartSchedOnNode{
			Node:          n,
			SystemService: SystemdSchedServiceName,
			Cause:         err.Error(),
		}
	}
	return nil
}

//EnableSchedulingOnNode enable apps to be scheduled to a given k8s worker node
func (k *K8s) EnableSchedulingOnNode(n node.Node) error {
	return k8s_ops.Instance().UnCordonNode(n.Name, DefaultTimeout, DefaultRetryInterval)
}

//DisableSchedulingOnNode disable apps to be scheduled to a given k8s worker node
func (k *K8s) DisableSchedulingOnNode(n node.Node) error {
	return k8s_ops.Instance().CordonNode(n.Name, DefaultTimeout, DefaultRetryInterval)
}

//IsScalable check whether scalable
func (k *K8s) IsScalable(spec interface{}) bool {
	if obj, ok := spec.(*apps_api.Deployment); ok {
		dep, err := k8s_ops.Instance().GetDeployment(obj.Name, obj.Namespace)
		if err != nil {
			logrus.Errorf("Failed to retrieve deployment [%s] %s. Cause: %v", obj.Namespace, obj.Name, err)
			return false
		}
		for _, vol := range dep.Spec.Template.Spec.Volumes {
			pvcName := vol.PersistentVolumeClaim.ClaimName
			pvc, err := k8s_ops.Instance().GetPersistentVolumeClaim(pvcName, dep.Namespace)
			if err != nil {
				logrus.Errorf("Failed to retrieve PVC [%s] %s. Cause: %v", obj.Namespace, pvcName, err)
				return false
			}
			for _, ac := range pvc.Spec.AccessModes {
				if ac == v1.ReadWriteOnce {
					return false
				}
			}
		}
	} else if _, ok := spec.(*apps_api.StatefulSet); ok {
		return true
	}
	return false
}

// GetTokenFromConfigMap -  Retrieve the config map object and get auth-token
func (k *K8s) GetTokenFromConfigMap(configMapName string) (string, error) {
	var token string
	var err error
	var configMap *v1.ConfigMap
	k8sOps := k8s_ops.Instance()
	if configMap, err = k8sOps.GetConfigMap(configMapName, "default"); err == nil {
		if secret, err := k8sOps.GetSecret(configMap.Data[secretNameKey], configMap.Data[secretNamespaceKey]); err == nil {
			if tk, ok := secret.Data["auth-token"]; ok {
				token = string(tk)
			}
		}
	}
	logrus.Infof("Token from secret: %s", token)
	return token, err
}

func (k *K8s) createMigrationObjects(
	specObj interface{},
	ns *v1.Namespace,
	app *spec.AppSpec,
) (interface{}, error) {
	k8sOps := k8s_ops.Instance()
	if obj, ok := specObj.(*stork_api.ClusterPair); ok {
		obj.Namespace = ns.Name
		clusterPair, err := k8sOps.CreateClusterPair(obj)
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create ClusterPair: %v. Err: %v", obj.Name, err),
			}
		}
		logrus.Infof("[%v] Created ClusterPair: %v", app.Key, clusterPair.Name)
		return clusterPair, nil
	} else if obj, ok := specObj.(*stork_api.Migration); ok {
		obj.Namespace = ns.Name
		migration, err := k8sOps.CreateMigration(obj)
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create Migration: %v. Err: %v", obj.Name, err),
			}
		}
		logrus.Infof("[%v] Created Migration: %v", app.Key, migration.Name)
		return migration, nil
	} else if obj, ok := specObj.(*stork_api.MigrationSchedule); ok {
		obj.Namespace = ns.Name
		migrationSchedule, err := k8sOps.CreateMigrationSchedule(obj)
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create MigrationSchedule: %v. Err: %v", obj.Name, err),
			}
		}
		logrus.Infof("[%v] Created MigrationSchedule: %v", app.Key, migrationSchedule.Name)
		return migrationSchedule, nil
	} else if obj, ok := specObj.(*stork_api.SchedulePolicy); ok {
		schedPolicy, err := k8sOps.CreateSchedulePolicy(obj)
		if errors.IsAlreadyExists(err) {
			if schedPolicy, err = k8sOps.GetSchedulePolicy(obj.Name); err == nil {
				logrus.Infof("[%v] Found existing schedule policy: %v", app.Key, schedPolicy.Name)
				return schedPolicy, nil
			}
		}

		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create SchedulePolicy: %v. Err: %v", obj.Name, err),
			}
		}
		logrus.Infof("[%v] Created SchedulePolicy: %v", app.Key, schedPolicy.Name)
		return schedPolicy, nil
	}

	return nil, nil
}

func (k *K8s) getPodsUsingStorage(pods []v1.Pod, provisioner string) []v1.Pod {
	k8sOps := k8s_ops.Instance()
	podsUsingStorage := make([]v1.Pod, 0)
	for _, pod := range pods {
		for _, vol := range pod.Spec.Volumes {
			if vol.PersistentVolumeClaim == nil {
				continue
			}
			pvc, err := k8sOps.GetPersistentVolumeClaim(vol.PersistentVolumeClaim.ClaimName, pod.Namespace)
			if err != nil {
				logrus.Errorf("failed to get pvc [%s] %s. Cause: %v", vol.PersistentVolumeClaim.ClaimName, pod.Namespace, err)
				return podsUsingStorage
			}
			if scProvisioner, err := k8sOps.GetStorageProvisionerForPVC(pvc); err == nil && scProvisioner == volume.GetStorageProvisioner() {
				podsUsingStorage = append(podsUsingStorage, pod)
				break
			}
		}
	}
	return podsUsingStorage
}

//PrepareNodeToDecommission Prepare the Node for decommission
func (k *K8s) PrepareNodeToDecommission(n node.Node, provisioner string) error {
	k8sOps := k8s_ops.Instance()
	pods, err := k8sOps.GetPodsByNode(n.Name, "")
	if err != nil {
		return &scheduler.ErrFailedToDecommissionNode{
			Node:  n,
			Cause: fmt.Sprintf("Failed to get pods on the node: %v. Err: %v", n.Name, err),
		}
	}
	podsUsingStorage := k.getPodsUsingStorage(pods.Items, provisioner)
	// double the timeout every 40 pods
	timeout := DefaultTimeout * time.Duration(len(podsUsingStorage)/40+1)
	if err = k8sOps.DrainPodsFromNode(n.Name, podsUsingStorage, timeout, DefaultRetryInterval); err != nil {
		return &scheduler.ErrFailedToDecommissionNode{
			Node:  n,
			Cause: fmt.Sprintf("Failed to drain pods from node: %v. Err: %v", n.Name, err),
		}
	}
	return nil
}

func (k *K8s) destroyMigrationObject(
	specObj interface{},
	app *spec.AppSpec,
) error {
	k8sOps := k8s_ops.Instance()
	if obj, ok := specObj.(*stork_api.ClusterPair); ok {
		err := k8sOps.DeleteClusterPair(obj.Name, obj.Namespace)
		if err != nil {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to delete ClusterPair: %v. Err: %v", obj.Name, err),
			}
		}
		logrus.Infof("[%v] Destroyed ClusterPair: %v", app.Key, obj.Name)
	} else if obj, ok := specObj.(*stork_api.Migration); ok {
		err := k8sOps.DeleteMigration(obj.Name, obj.Namespace)
		if err != nil {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to delete Migration: %v. Err: %v", obj.Name, err),
			}
		}
		logrus.Infof("[%v] Destroyed Migration: %v", app.Key, obj.Name)
	} else if obj, ok := specObj.(*stork_api.MigrationSchedule); ok {
		err := k8sOps.DeleteMigrationSchedule(obj.Name, obj.Namespace)
		if err != nil {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to delete MigrationSchedule: %v. Err: %v", obj.Name, err),
			}
		}
		logrus.Infof("[%v] Destroyed MigrationSchedule: %v", app.Key, obj.Name)
	} else if obj, ok := specObj.(*stork_api.SchedulePolicy); ok {
		err := k8sOps.DeleteSchedulePolicy(obj.Name)
		if err != nil {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to delete SchedulePolicy: %v. Err: %v", obj.Name, err),
			}
		}
		logrus.Infof("[%v] Destroyed SchedulePolicy: %v", app.Key, obj.Name)
	}
	return nil
}

func (k *K8s) destroyVolumeSnapshotRestoreObject(
	specObj interface{},
	app *spec.AppSpec,
) error {
	k8sOps := k8s_ops.Instance()
	if obj, ok := specObj.(*stork_api.VolumeSnapshotRestore); ok {
		err := k8sOps.DeleteVolumeSnapshotRestore(obj.Name, obj.Namespace)
		if err != nil {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to delete VolumeSnapshotRestore: %v. Err: %v", obj.Name, err),
			}
		}
		logrus.Infof("[%v] Destroyed VolumeSnapshotRestore: %v", app.Key, obj.Name)
	}
	return nil
}

// ValidateVolumeSnapshotRestore return nil if snapshot is restored successuflly to
// parent volumes
func (k *K8s) ValidateVolumeSnapshotRestore(ctx *scheduler.Context, timeStart time.Time) error {
	var err error
	var snapRestore *stork_api.VolumeSnapshotRestore
	if ctx == nil {
		return fmt.Errorf("no context provided")
	}
	// extract volume name and snapshotname from context
	// can do it using snapRestore.Status.Volume
	k8sOps := k8s_ops.Instance()
	specObjects := ctx.App.SpecList
	driver, err := volume.Get(k.VolDriverName)
	if err != nil {
		return err
	}

	for _, specObj := range specObjects {
		if obj, ok := specObj.(*stork_api.VolumeSnapshotRestore); ok {
			snapRestore, err = k8sOps.GetVolumeSnapshotRestore(obj.Name, obj.Namespace)
			if err != nil {
				return fmt.Errorf("unable to restore volumesnapshotrestore details %v", err)
			}
			err = k8sOps.ValidateVolumeSnapshotRestore(snapRestore.Name, snapRestore.Namespace, DefaultTimeout, DefaultRetryInterval)
			if err != nil {
				return err
			}

		}
	}
	if snapRestore == nil {
		return fmt.Errorf("no valid volumesnapshotrestore specs found")
	}

	for _, vol := range snapRestore.Status.Volumes {
		logrus.Infof("validating volume %v is restored from %v", vol.Volume, vol.Snapshot)
		snapshotData, err := k8sOps.GetSnapshotData(vol.Snapshot)
		if err != nil {
			return fmt.Errorf("failed to retrieve VolumeSnapshotData %s: %v",
				vol.Snapshot, err)
		}
		err = k8sOps.ValidateSnapshotData(snapshotData.Metadata.Name, false, DefaultTimeout, DefaultRetryInterval)
		if err != nil {
			return fmt.Errorf("snapshot: %s is not complete. %v", snapshotData.Metadata.Name, err)
		}
		// validate each snap restore
		if err := driver.ValidateVolumeSnapshotRestore(vol.Volume, snapshotData, timeStart); err != nil {
			return err
		}
	}

	return nil
}

func (k *K8s) createBackupObjects(
	specObj interface{},
	ns *v1.Namespace,
	app *spec.AppSpec,
) (interface{}, error) {
	k8sOps := k8s_ops.Instance()
	if obj, ok := specObj.(*stork_api.BackupLocation); ok {
		obj.Namespace = ns.Name
		backupLocation, err := k8sOps.CreateBackupLocation(obj)
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create BackupLocation: %v. Err: %v", obj.Name, err),
			}
		}
		logrus.Infof("[%v] Created BackupLocation: %v", app.Key, backupLocation.Name)
		return backupLocation, nil
	} else if obj, ok := specObj.(*stork_api.ApplicationBackup); ok {
		obj.Namespace = ns.Name
		applicationBackup, err := k8sOps.CreateApplicationBackup(obj)
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create ApplicationBackup: %v. Err: %v", obj.Name, err),
			}
		}
		logrus.Infof("[%v] Created ApplicationBackup: %v", app.Key, applicationBackup.Name)
		return applicationBackup, nil
	} else if obj, ok := specObj.(*stork_api.ApplicationRestore); ok {
		obj.Namespace = ns.Name
		applicationRestore, err := k8sOps.CreateApplicationRestore(obj)
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create ApplicationRestore: %v. Err: %v", obj.Name, err),
			}
		}
		logrus.Infof("[%v] Created ApplicationRestore: %v", app.Key, applicationRestore.Name)
		return applicationRestore, nil
	} else if obj, ok := specObj.(*stork_api.ApplicationClone); ok {
		applicationClone, err := k8sOps.CreateApplicationClone(obj)
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create ApplicationClone: %v. Err: %v", obj.Name, err),
			}
		}
		logrus.Infof("[%v] Created ApplicationClone: %v", app.Key, applicationClone.Name)
		return applicationClone, nil
	}
	return nil, nil
}

func (k *K8s) destroyBackupObjects(
	specObj interface{},
	app *spec.AppSpec,
) error {
	k8sOps := k8s_ops.Instance()
	if obj, ok := specObj.(*stork_api.BackupLocation); ok {
		err := k8sOps.DeleteBackupLocation(obj.Name, obj.Namespace)
		if err != nil {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to delete BackupLocation: %v. Err: %v", obj.Name, err),
			}
		}
		logrus.Infof("[%v] Destroyed BackupLocation: %v", app.Key, obj.Name)
	} else if obj, ok := specObj.(*stork_api.ApplicationBackup); ok {
		err := k8sOps.DeleteApplicationBackup(obj.Name, obj.Namespace)
		if err != nil {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to delete ApplicationBackup: %v. Err: %v", obj.Name, err),
			}
		}
		logrus.Infof("[%v] Destroyed ApplicationBackup: %v", app.Key, obj.Name)
	} else if obj, ok := specObj.(*stork_api.ApplicationRestore); ok {
		err := k8sOps.DeleteApplicationRestore(obj.Name, obj.Namespace)
		if err != nil {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to delete ApplicationRestore: %v. Err: %v", obj.Name, err),
			}
		}
		logrus.Infof("[%v] Destroyed ApplicationRestore: %v", app.Key, obj.Name)
	} else if obj, ok := specObj.(*stork_api.ApplicationClone); ok {
		err := k8sOps.DeleteApplicationClone(obj.Name, obj.Namespace)
		if err != nil {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to delete ApplicationClone: %v. Err: %v", obj.Name, err),
			}
		}
		logrus.Infof("[%v] Destroyed ApplicationClone: %v", app.Key, obj.Name)
	}
	return nil
}

func insertLineBreak(note string) string {
	return fmt.Sprintf("------------------------------\n%s\n------------------------------\n", note)
}

func dumpPodStatusRecursively(pod v1.Pod) string {
	var buf bytes.Buffer
	buf.WriteString(insertLineBreak(pod.Name))
	buf.WriteString(fmt.Sprintf("%v\n", pod.Status))
	for _, conStat := range pod.Status.ContainerStatuses {
		buf.WriteString(insertLineBreak(conStat.Name))
		buf.WriteString(fmt.Sprintf("%v\n", conStat))
		buf.WriteString(insertLineBreak("END container"))
	}
	buf.WriteString(insertLineBreak("END pod"))
	return buf.String()
}

func init() {
	k := &K8s{}
	scheduler.Register(SchedName, k)
}
