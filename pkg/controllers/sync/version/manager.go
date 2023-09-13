/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

This file may have been modified by The KubeAdmiral Authors
("KubeAdmiral Modifications"). All KubeAdmiral Modifications
are Copyright 2023 The KubeAdmiral Authors.
*/

package version

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	runtimeclient "sigs.k8s.io/controller-runtime/pkg/client"

	fedcorev1a1 "github.com/kubewharf/kubeadmiral/pkg/apis/core/v1alpha1"
	fedcorev1a1client "github.com/kubewharf/kubeadmiral/pkg/client/clientset/versioned/typed/core/v1alpha1"
	"github.com/kubewharf/kubeadmiral/pkg/controllers/common"
	"github.com/kubewharf/kubeadmiral/pkg/controllers/sync/propagatedversion"
)

// VersionedResource defines the methods a federated resource must
// implement to allow versions to be tracked by the VersionManager.
type VersionedResource interface {
	// FederatedName returns the qualified name of the underlying
	// FederatedObject or ClusterFederatedObject.
	FederatedName() common.QualifiedName
	// Object returns the underlying FederatedObject or ClusterFederatedObject
	// as a GenericFederatedObject.
	Object() fedcorev1a1.GenericFederatedObject
	// TemplateVersion returns the resource's current template version.
	TemplateVersion() (string, error)
	// OverrideVersion returns the resource's current override version.
	OverrideVersion() (string, error)
	// FederatedGVK returns the GroupVersionKind of the underlying
	// FederatedObject or ClusterFederatedObject.
	FederatedGVK() schema.GroupVersionKind
}

/*
VersionManager is used by the Sync controller to record the last synced version
of a FederatedObject along with the versions of the cluster objects that were
created/updated in the process. This is important in preventing unnecessary
update requests from being sent to member clusters in subsequent reconciles. The
VersionManager persists this information in the apiserver in the form of
PropagatedVersion/ClusterPropagatedVersions, see
pkg/apis/types_propagatedversion.go.

In the context of the Sync controller, we identify the "version" of a
FederatedObject with the hash of its template and overrides and we identify the
"version" of a cluster object to be either its Generation (if available) or its
ResourceVersion.

VersionManager is required because created/updated cluster objects might not
match the template exactly due to various reasons such as default values,
admission plugins or webhooks. Thus we have to store the version returned by the
create/update request to avoid false-positives when determining if the cluster
object has diverged from the template in subsequent reconciles.
*/

// VersionManager 用于记录 FederatedObject 最后同步到成员集群的版本,
// 以 PropagatedVersion/ClusterPropagatedVersions 对象的形式持久化到 Kubernetes 中
// VersionManager 为 resourceAccessor 中的一个字段，存储所有 FederatedObject 当前分发的版本信息
type VersionManager struct {
	sync.RWMutex

	// Namespace to source propagated versions from
	namespace string

	adapter VersionAdapter

	hasSynced bool

	// key 是 fedResource 的qualifiedName，value 是 ClusterPropagatedVersion / PropagatedVersion 对象
	versions map[string]runtimeclient.Object

	client fedcorev1a1client.CoreV1alpha1Interface

	logger klog.Logger
}

func NewNamespacedVersionManager(
	logger klog.Logger,
	client fedcorev1a1client.CoreV1alpha1Interface,
	namespace string,
) *VersionManager {
	adapter := NewVersionAdapter(true)
	v := &VersionManager{
		logger:    logger.WithValues("origin", "version-manager", "type-name", adapter.TypeName()),
		namespace: namespace,
		adapter:   adapter,
		versions:  make(map[string]runtimeclient.Object),
		client:    client,
	}

	return v
}

func NewClusterVersionManager(
	logger klog.Logger,
	client fedcorev1a1client.CoreV1alpha1Interface,
) *VersionManager {
	adapter := NewVersionAdapter(false)
	v := &VersionManager{
		logger:    logger.WithValues("origin", "version-manager", "type-name", adapter.TypeName()),
		namespace: "",
		adapter:   adapter,
		versions:  make(map[string]runtimeclient.Object),
		client:    client,
	}

	return v
}

// Sync retrieves propagated versions from the api and loads it into
// memory.
func (m *VersionManager) Sync(ctx context.Context) {
	versionList, ok := m.list(ctx)
	if !ok {
		return
	}
	m.load(ctx, versionList)
}

// HasSynced indicates whether the manager's in-memory state has been
// synced with the api.
func (m *VersionManager) HasSynced() bool {
	m.RLock()
	defer m.RUnlock()
	return m.hasSynced
}

// Get retrieves a mapping of cluster names to versions for the given versioned
// resource. It returns an empty map if the desired object for the versioned
// resource is different from last recorded.
func (m *VersionManager) Get(resource VersionedResource) (map[string]string, error) {
	versionMap := make(map[string]string)

	qualifiedName := m.versionQualifiedName(resource.FederatedName())
	key := qualifiedName.String()
	m.RLock()
	obj, ok := m.versions[key]
	m.RUnlock()
	if !ok {
		return versionMap, nil
	}
	status := m.adapter.GetStatus(obj)

	templateVersion, err := resource.TemplateVersion()
	if err != nil {
		return nil, errors.Wrap(err, "Failed to determine template version")
	}
	overrideVersion, err := resource.OverrideVersion()
	if err != nil {
		return nil, errors.Wrap(err, "Failed to determine override version")
	}
	if templateVersion == status.TemplateVersion &&
		overrideVersion == status.OverrideVersion {
		for _, versions := range status.ClusterVersions {
			versionMap[versions.ClusterName] = versions.Version
		}
	}

	return versionMap, nil
}

// Update 根据传入的信息，在k8s中创建或更新 ClusterPropagatedVersion/PropagatedVersion 对象
// resource 是federatedObject,
// selectedClusters 是此次待同步的集群,
// versionMap 是最新的各集群分发资源版本号
// Update ensures that the propagated version for the given versioned
// resource is recorded.
func (m *VersionManager) Update(
	resource VersionedResource,
	selectedClusters []string,
	versionMap map[string]string,
) error {
	// 0. 获取前置信息
	// 将 federatedObject.Spec.Template hash出一个版本号
	templateVersion, err := resource.TemplateVersion()
	if err != nil {
		return errors.Wrap(err, "Failed to determine template version")
	}
	// 将 federatedObject.Spec.Override hash出一个版本号
	overrideVersion, err := resource.OverrideVersion()
	if err != nil {
		return errors.Wrap(err, "Failed to determine override version")
	}
	// 此处就是获取 federatedObject 的 qualifiedName
	qualifiedName := m.versionQualifiedName(resource.FederatedName())
	key := qualifiedName.String()

	m.Lock()

	// 1. 构建或更新 obj(ClusterPropagatedVersion/PropagatedVersion) 的字段
	// 1.1 从缓存中获取旧的 ClusterPropagatedVersion / PropagatedVersion 对象
	obj, ok := m.versions[key]

	var oldStatus *fedcorev1a1.PropagatedVersionStatus
	var clusterVersions []fedcorev1a1.ClusterObjectVersion

	// 1.2 根据传入的 versionMap 构建 PropagatedVersionStatus.ClusterVersions 字段
	if ok {
		oldStatus = m.adapter.GetStatus(obj)
		// 如果 template and override 都没变过，则保留那些已存在的版本信息
		// The existing versions are still valid if the template and override versions match.
		if oldStatus.TemplateVersion == templateVersion && oldStatus.OverrideVersion == overrideVersion {
			clusterVersions = oldStatus.ClusterVersions
		}
		clusterVersions = updateClusterVersions(clusterVersions, versionMap, selectedClusters)
	} else {
		// 直接使用传入的最新分发资源版本信息
		clusterVersions = VersionMapToClusterVersions(versionMap)
	}

	// status 是 ClusterPropagatedVersion/PropagatedVersion.Status 字段
	status := &fedcorev1a1.PropagatedVersionStatus{
		TemplateVersion: templateVersion,
		OverrideVersion: overrideVersion,
		ClusterVersions: clusterVersions,
	}

	// 如果新、旧传播版本内容一致，则无需更新，打印一个日志即可
	if oldStatus != nil && propagatedversion.PropagatedVersionStatusEquivalent(oldStatus, status) {
		m.Unlock()
		m.logger.WithValues("version-qualified-name", qualifiedName).
			V(4).Info("No need to update propagated version status")
		return nil
	}

	// 构建或更新 ClusterPropagatedVersion 或 PropagatedVersion 对象
	if obj == nil {
		// 构建一个 ClusterPropagatedVersion 或 PropagatedVersion 对象
		ownerReference := ownerReferenceForFederatedObject(resource)
		obj = m.adapter.NewVersion(qualifiedName, ownerReference, status)
		// 将该 obj 缓存到 VersionManager.versions 中
		m.versions[key] = obj
	} else {
		// 设置 ClusterPropagatedVersion/PropagatedVersion.Status 字段
		m.adapter.SetStatus(obj, status)
	}

	m.Unlock()

	// 2. 在k8s中更新或创建 obj
	// Since writeVersion calls the Kube API, the manager should be
	// unlocked to avoid blocking on calls across the network.

	return m.writeVersion(obj, qualifiedName)
}

// Delete removes the named propagated version from the manager.
// Versions are written to the API with an owner reference to the
// versioned resource, and they should be removed by the garbage
// collector when the resource is removed.
func (m *VersionManager) Delete(qualifiedName common.QualifiedName) {
	versionQualifiedName := m.versionQualifiedName(qualifiedName)
	m.Lock()
	delete(m.versions, versionQualifiedName.String())
	m.Unlock()
}

func (m *VersionManager) list(ctx context.Context) (runtimeclient.ObjectList, bool) {
	// Attempt retrieval of list of versions until success or the channel is closed.
	var versionList runtimeclient.ObjectList
	err := wait.PollImmediateInfiniteWithContext(ctx, 1*time.Second, func(ctx context.Context) (bool, error) {
		var err error
		versionList, err = m.adapter.List(
			ctx, m.client, m.namespace, metav1.ListOptions{
				ResourceVersion: "0",
			})
		if err != nil {
			m.logger.Error(err, "Failed to list propagated versions")
			// Do not return the error to allow the operation to be retried.
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		return nil, false
	}
	return versionList, true
}

// load processes a list of versions into in-memory cache.  Since the
// version manager should not be used in advance of HasSynced
// returning true, locking is assumed to be unnecessary.
func (m *VersionManager) load(ctx context.Context, versionList runtimeclient.ObjectList) bool {
	objs, err := meta.ExtractList(versionList)
	if err != nil {
		m.logger.Error(err, "Failed to extract version list")
		return false
	}
	for _, obj := range objs {
		select {
		case <-ctx.Done():
			m.logger.Info("Halting version manager load due to closed stop channel")
			return false
		default:
		}

		obj := obj.(runtimeclient.Object)
		qualifiedName := common.NewQualifiedName(obj)
		m.versions[qualifiedName.String()] = obj
	}
	m.Lock()
	m.hasSynced = true
	m.Unlock()
	m.logger.Info("Version manager synced")
	return true
}

// versionQualifiedName derives the qualified name of a version
// resource from the qualified name of a federated object.
func (m *VersionManager) versionQualifiedName(qualifiedName common.QualifiedName) common.QualifiedName {
	return qualifiedName
}

// writeVersion 将传入的 obj 在 k8s 中创建或更新obj.Status字段
// writeVersion serializes the current state of the named propagated
// version to the API.
//
// The manager is expected to be called synchronously by the sync
// controller which should ensure that the version object for a given
// resource is updated by at most one thread at a time.  This should
// guarantee safe manipulation of an object retrieved from the
// version map.
func (m *VersionManager) writeVersion(obj runtimeclient.Object, qualifiedName common.QualifiedName) error {
	key := qualifiedName.String()
	// 获取 obj 的类型 （ClusterPropagatedVersion/PropagatedVersion）
	adapterType := m.adapter.TypeName()
	keyedLogger := m.logger.WithValues("version-qualified-name", key)

	// 获取 obj.metadata.ResourceVersion 字段
	resourceVersion := getResourceVersion(obj)
	refreshVersion := false
	// TODO Centralize polling interval and duration
	waitDuration := 30 * time.Second
	err := wait.PollImmediate(100*time.Millisecond, waitDuration, func() (bool, error) {
		var err error

		// 如果 refreshVersion 变为 true，则从 k8s 中获取最新的 obj，并更新 resourceVersion 字段
		if refreshVersion {
			// Version was written to the API by another process after the last manager write.
			resourceVersion, err = m.getResourceVersionFromAPI(qualifiedName)
			if err != nil {
				keyedLogger.Error(err, "Failed to refresh the resourceVersion from the API")
				return false, nil
			}
			refreshVersion = false
		}

		// 如果 resourceVersion 为空，表示obj是新构造出来的，需要在k8s中创建
		if resourceVersion == "" {
			// Version resource needs to be created

			createdObj := obj.DeepCopyObject().(runtimeclient.Object)
			setResourceVersion(createdObj, "")
			keyedLogger.V(1).Info("Creating resourceVersion")
			createdObj, err = m.adapter.Create(context.TODO(), m.client, createdObj, metav1.CreateOptions{})
			// 如果k8s中已存在该对象，则设置refreshVersion为true，并进行重试（重试时会从k8s中获取最新的obj）
			if apierrors.IsAlreadyExists(err) {
				keyedLogger.V(1).Info("ResourceVersion was created by another process. Will refresh the resourceVersion and attempt to update")
				refreshVersion = true
				return false, nil
			}
			// Forbidden is likely to be a permanent failure and
			// likely the result of the containing namespace being
			// deleted.
			if apierrors.IsForbidden(err) {
				return false, err
			}
			if err != nil {
				keyedLogger.Error(err, "Failed to create resourceVersion")
				return false, nil
			}

			// k8s 创建好后，获取 resourceVersion 字段
			// Update the resource version that will be used for update.
			resourceVersion = getResourceVersion(createdObj)
		}

		// 更新 k8s 中 obj 的 status 字段
		// Update the status of an existing object

		updatedObj := obj.DeepCopyObject().(runtimeclient.Object)
		setResourceVersion(updatedObj, resourceVersion)

		keyedLogger.V(1).Info("Updating the status")
		updatedObj, err = m.adapter.UpdateStatus(context.TODO(), m.client, updatedObj, metav1.UpdateOptions{})
		if apierrors.IsConflict(err) {
			keyedLogger.V(1).Info("ResourceVersion was updated by another process. Will refresh the resourceVersion and retry the update")
			refreshVersion = true
			return false, nil
		}
		if apierrors.IsNotFound(err) {
			keyedLogger.V(1).Info("ResourceVersion was deleted by another process. Will clear the resourceVersion and retry the update")
			resourceVersion = ""
			return false, nil
		}
		// Forbidden is likely to be a permanent failure and
		// likely the result of the containing namespace being
		// deleted.
		if apierrors.IsForbidden(err) {
			return false, err
		}
		if err != nil {
			keyedLogger.Error(err, "Failed to update the status")
			return false, nil
		}

		// Update was successful. All returns should be true even in
		// the event of an error since the next reconcile can also
		// refresh the resource version if necessary.

		// Update the version resource
		resourceVersion = getResourceVersion(updatedObj)
		setResourceVersion(obj, resourceVersion)

		return true, nil
	})
	if err != nil {
		return errors.Wrapf(err, "Failed to write the version map for %s %q to the API", adapterType, key)
	}
	return nil
}

func (m *VersionManager) getResourceVersionFromAPI(qualifiedName common.QualifiedName) (string, error) {
	m.logger.V(2).Info("Retrieving resourceVersion from the API", "version-qualified-name", qualifiedName)
	obj, err := m.adapter.Get(context.TODO(), m.client, qualifiedName.Namespace, qualifiedName.Name, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	return getResourceVersion(obj), nil
}

func getResourceVersion(obj runtimeclient.Object) string {
	return obj.GetResourceVersion()
}

func setResourceVersion(obj runtimeclient.Object, resourceVersion string) {
	obj.SetResourceVersion(resourceVersion)
}

func ownerReferenceForFederatedObject(resource VersionedResource) metav1.OwnerReference {
	gvk := resource.FederatedGVK()
	obj := resource.Object()
	return metav1.OwnerReference{
		APIVersion: gvk.GroupVersion().String(),
		Kind:       gvk.Kind,
		Name:       obj.GetName(),
		UID:        obj.GetUID(),
	}
}

// 整体以 newVersions 中为主，其中
// 如果 newVersions 中缺失了部分集群信息（当前需要同步的集群：selectedClusters），则从 oldVersions 中查找并不上
// 作用：可能部分子集群中的对象并未更新，导致 newVersions 中没有记录，则直接从 oldVersions 取出不上
func updateClusterVersions(
	oldVersions []fedcorev1a1.ClusterObjectVersion,
	newVersions map[string]string,
	selectedClusters []string,
) []fedcorev1a1.ClusterObjectVersion {
	// Retain versions for selected clusters that were not changed
	selectedClusterSet := sets.NewString(selectedClusters...)
	// 遍历 oldVersions，如果没有 newVersions 中没有，且是此次的待同步集群，则使用旧版本的信息给 newVersions不上
	for _, oldVersion := range oldVersions {
		if !selectedClusterSet.Has(oldVersion.ClusterName) {
			continue
		}
		if _, ok := newVersions[oldVersion.ClusterName]; !ok {
			newVersions[oldVersion.ClusterName] = oldVersion.Version
		}
	}

	// 将map变为了排序好的列表
	return VersionMapToClusterVersions(newVersions)
}

// VersionMapToClusterVersions 将 版本号map 变为了按照 clusterName 大小排序的 版本号列表
func VersionMapToClusterVersions(versionMap map[string]string) []fedcorev1a1.ClusterObjectVersion {
	clusterVersions := []fedcorev1a1.ClusterObjectVersion{}
	for clusterName, version := range versionMap {
		// Lack of version indicates deletion
		if version == "" {
			continue
		}
		clusterVersions = append(clusterVersions, fedcorev1a1.ClusterObjectVersion{
			ClusterName: clusterName,
			Version:     version,
		})
	}
	propagatedversion.SortClusterVersions(clusterVersions)
	return clusterVersions
}
