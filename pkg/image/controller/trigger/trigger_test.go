package trigger

import (
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	kapi "k8s.io/kubernetes/pkg/api"
	kapierrs "k8s.io/kubernetes/pkg/api/errors"
	"k8s.io/kubernetes/pkg/api/meta"
	"k8s.io/kubernetes/pkg/api/unversioned"
	"k8s.io/kubernetes/pkg/client/cache"
	"k8s.io/kubernetes/pkg/client/record"
	"k8s.io/kubernetes/pkg/labels"
	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/util/diff"
	"k8s.io/kubernetes/pkg/watch"

	buildapi "github.com/openshift/origin/pkg/build/api"
	deployapi "github.com/openshift/origin/pkg/deploy/api"
	imageapi "github.com/openshift/origin/pkg/image/api"
	"github.com/openshift/origin/pkg/image/api/v1/trigger"
	"sort"
)

type queuedOp struct {
	key     string
	changes []ImageChange
}

type mockOperationQueue struct {
	lock   sync.Mutex
	queued []queuedOp
}

func (q *mockOperationQueue) Add(key string, changes []ImageChange) {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.queued = append(q.queued, queuedOp{key, changes})
}
func (q *mockOperationQueue) AddRateLimited(key string, changes []ImageChange)            {}
func (q *mockOperationQueue) AddAfter(key string, changes []ImageChange, d time.Duration) {}
func (q *mockOperationQueue) NumRequeues(key string) int                                  { return 0 }
func (q *mockOperationQueue) Get() (key string, opts []ImageChange, shutdown bool) {
	return "", nil, false
}
func (q *mockOperationQueue) Done(key string)   {}
func (q *mockOperationQueue) Forget(key string) {}
func (q *mockOperationQueue) All() []queuedOp {
	q.lock.Lock()
	defer q.lock.Unlock()
	return q.queued
}
func (q *mockOperationQueue) Len() int {
	q.lock.Lock()
	defer q.lock.Unlock()
	return len(q.queued)
}

type streamTagResults struct {
	ref string
	rv  int64
}
type namespaceTags map[string]streamTagResults
type mockTags map[string]namespaceTags

type mockTagRetriever struct {
	calls int
	tags  mockTags
}

func (r *mockTagRetriever) ImageStreamTag(namespace, name string) (string, int64, bool) {
	r.calls++
	if i, ok := r.tags[namespace]; ok {
		if j, ok := i[name]; ok {
			return j.ref, j.rv, true
		}
	}
	return "", 0, false
}

type mockImageStreamLister struct {
	namespace string

	stream *imageapi.ImageStream
	err    error
}

func (l *mockImageStreamLister) List(selector labels.Selector) (ret []*imageapi.ImageStream, err error) {
	return nil, l.err
}
func (l *mockImageStreamLister) ImageStreams(namespace string) ImageStreamNamespaceLister {
	l.namespace = namespace
	return l
}
func (l *mockImageStreamLister) Get(name string) (*imageapi.ImageStream, error) {
	return l.stream, l.err
}

type fakeInstantiator struct {
	build *buildapi.Build
	err   error

	namespace string
	req       *buildapi.BuildRequest
}

func (i *fakeInstantiator) Instantiate(namespace string, req *buildapi.BuildRequest) (*buildapi.Build, error) {
	i.req, i.namespace = req, namespace
	return i.build, i.err
}

func queuedEqual(a, b []queuedOp) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].key != b[i].key {
			return false
		}
		if !operationsEqual(a[i].changes, b[i].changes) {
			return false
		}
	}
	return true
}

func operationsEqual(a, b []ImageChange) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !operationEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}

func operationEqual(a, b ImageChange) bool {
	switch {
	case a.Trigger != nil && b.Trigger != nil:
	case a.Trigger != nil, b.Trigger != nil:
		return false
	}
	return a.Name == b.Name && a.Namespace == b.Namespace && a.RV == b.RV && a.Ref == b.Ref && a.Tag == b.Tag
}

func TestTriggerControllerSyncImageStream(t *testing.T) {
	empty := &trigger.ObjectFieldTrigger{}
	queue := &mockOperationQueue{}
	lister := &mockImageStreamLister{
		stream: scenario_1_imageStream_single("test", "stream", "10"),
	}
	controller := TriggerController{
		triggerCache:     NewTriggerCache(),
		lister:           lister,
		imageChangeQueue: queue,
	}
	controller.triggerCache.Add("buildconfigs/test/build1", &TriggerCacheEntry{
		Key:       "buildconfigs/test/build1",
		Namespace: "test",
		Triggers: []trigger.ObjectFieldTrigger{
			{From: trigger.ObjectReference{Kind: "ImageStreamTag", Name: "stream:1"}},
			{From: trigger.ObjectReference{Kind: "ImageStreamTag", Name: "stream:2"}},
			{From: trigger.ObjectReference{Kind: "DockerImage", Name: "test/stream:1"}},
		},
	})
	if err := controller.syncImageStream("test/stream"); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	queued := queue.All()
	if len(queued) != 1 || queued[0].key != "buildconfigs/test/build1" || !operationsEqual(queued[0].changes, []ImageChange{
		{Name: "stream:1", Tag: "1", RV: 10, Namespace: "test", Ref: "image/result:1", Trigger: empty},
	}) {
		t.Errorf("unexpected changes: %#v", queued)
	}
}

func TestTriggerControllerSyncBuildConfigResource(t *testing.T) {
	lister := &mockImageStreamLister{
		stream: scenario_1_imageStream_single("test", "stream", "10"),
	}
	store := &cache.FakeCustomStore{
		GetByKeyFunc: func(key string) (interface{}, bool, error) {
			return scenario_1_buildConfig_imageSource(), true, nil
		},
	}
	inst := &fakeInstantiator{}
	reaction := &BuildConfigReaction{
		Instantiator: inst,
	}
	controller := TriggerController{
		triggerCache: NewTriggerCache(),
		lister:       lister,
		triggerSources: map[string]TriggerSource{
			"buildconfigs": {
				Store:    store,
				Reaction: reaction,
			},
		},
	}
	if err := controller.syncResource("buildconfigs/test/build1", []ImageChange{
		{Name: "stream:1", Tag: "1", RV: 10, Namespace: "test", Ref: "image/result:1", Trigger: &trigger.ObjectFieldTrigger{
			From:      trigger.ObjectReference{Kind: "ImageStreamTag", Name: "stream:1"},
			FieldPath: "spec.triggers.*.from",
		}},
	}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	expected := &buildapi.BuildRequest{
		ObjectMeta:       kapi.ObjectMeta{Name: "build2", Namespace: "test2"},
		From:             &kapi.ObjectReference{Kind: "DockerImage", Name: "mysql", Namespace: "other"},
		TriggeredByImage: &kapi.ObjectReference{Kind: "DockerImage", Name: "image/result:1"},
		TriggeredBy: []buildapi.BuildTriggerCause{
			{
				Message: "Image change",
				ImageChangeBuild: &buildapi.ImageChangeCause{
					FromRef: &kapi.ObjectReference{Kind: "DockerImage", Name: "mysql", Namespace: "other"},
				},
			},
		},
	}
	if inst.namespace != "test2" || !reflect.DeepEqual(inst.req, expected) {
		t.Errorf("unexpected: %s %s", inst.namespace, diff.ObjectReflectDiff(expected, inst.req))
	}
}

func TestBuildConfigTriggerIndexer(t *testing.T) {
	stopCh := make(chan struct{})
	defer close(stopCh)
	informer, fw := newFakeInformer(&buildapi.BuildConfig{}, &buildapi.BuildConfigList{ListMeta: unversioned.ListMeta{ResourceVersion: "1"}})

	empty := &trigger.ObjectFieldTrigger{}
	c := NewTriggerCache()
	r := &mockTagRetriever{}

	queue := &mockOperationQueue{}
	sources := []TriggerSource{
		{
			Resource: unversioned.GroupResource{Resource: "buildconfigs"},
			Informer: informer,
			TriggerFn: func(prefix string) TriggerIndexer {
				return buildConfigTriggerIndexer{prefix: prefix}
			},
		},
	}
	_, syncs, err := setupTriggerSources(c, r, sources, queue)
	if err != nil {
		t.Fatal(err)
	}
	go informer.Run(stopCh)
	if !cache.WaitForCacheSync(stopCh, syncs...) {
		t.Fatal("Unsynced")
	}

	// Verifies that two builds added to the informer:
	// - Perform a proper index of the triggers
	// - Queue the right changes, representing the changed/not-available images
	r.tags = mockTags{
		"test": namespaceTags{
			"stream:1": streamTagResults{ref: "image/result:1", rv: 10},
		},
		"other": namespaceTags{
			"stream:2": streamTagResults{ref: "image/result:2", rv: 11},
		},
	}
	fw.Add(scenario_1_buildConfig_strategy())
	fw.Add(scenario_1_buildConfig_imageSource())

	for len(c.List()) != 2 {
		time.Sleep(1 * time.Millisecond)
	}

	actual, ok := c.Get("buildconfigs/test/build1")
	if e := scenario_1_buildConfig_strategy_cacheEntry(); !ok || !reflect.DeepEqual(e, actual) {
		t.Fatalf("unexpected: %s", diff.ObjectReflectDiff(e, actual))
	}
	if err := verifyEntriesAt(c, []interface{}{scenario_1_buildConfig_strategy_cacheEntry()}, "test/stream"); err != nil {
		t.Fatal(err)
	}

	// verify we create two index entries and can cross namespaces with trigger types
	actual, ok = c.Get("buildconfigs/test2/build2")
	if e := scenario_1_buildConfig_imageSource_cacheEntry(); !ok || !reflect.DeepEqual(e, actual) {
		t.Fatalf("unexpected: %s", diff.ObjectReflectDiff(e, actual))
	}
	if err := verifyEntriesAt(c, []interface{}{scenario_1_buildConfig_imageSource_cacheEntry()}, "other/stream", "test2/stream"); err != nil {
		t.Fatal(err)
	}

	// should have enqueued a single action (based on the image stream tag retriever)
	queued := queue.All()
	expected := []queuedOp{
		{key: "buildconfigs/test/build1", changes: []ImageChange{
			{Name: "stream:1", Tag: "1", Ref: "image/result:1", RV: 10, Namespace: "test", Trigger: empty},
		}},
		{key: "buildconfigs/test2/build2", changes: []ImageChange{
			{Name: "stream:1", Tag: "1", RV: 0, Namespace: "test2", Trigger: empty},
		}},
	}
	if !queuedEqual(expected, queued) {
		t.Fatalf("changes: %#v", queued)
	}
}

func TestDeploymentConfigTriggerIndexer(t *testing.T) {
	stopCh := make(chan struct{})
	defer close(stopCh)
	informer, fw := newFakeInformer(&deployapi.DeploymentConfig{}, &deployapi.DeploymentConfigList{ListMeta: unversioned.ListMeta{ResourceVersion: "1"}})

	empty := &trigger.ObjectFieldTrigger{}
	c := NewTriggerCache()
	r := &mockTagRetriever{}

	queue := &mockOperationQueue{}
	sources := []TriggerSource{
		{
			Resource: unversioned.GroupResource{Resource: "deploymentconfigs"},
			Informer: informer,
			TriggerFn: func(prefix string) TriggerIndexer {
				return deploymentConfigTriggerIndexer{prefix: prefix}
			},
		},
	}
	_, syncs, err := setupTriggerSources(c, r, sources, queue)
	if err != nil {
		t.Fatal(err)
	}
	go informer.Run(stopCh)
	if !cache.WaitForCacheSync(stopCh, syncs...) {
		t.Fatal("Unsynced")
	}

	// Verifies that two builds added to the informer:
	// - Perform a proper index of the triggers
	// - Queue the right changes, representing the changed/not-available images
	r.tags = mockTags{
		"test": namespaceTags{
			"stream:1": streamTagResults{ref: "image/result:1", rv: 10},
		},
		"other": namespaceTags{
			"stream:2": streamTagResults{ref: "image/result:2", rv: 11},
		},
	}
	fw.Add(scenario_1_deploymentConfig_imageSource())

	for len(c.List()) != 1 {
		time.Sleep(1 * time.Millisecond)
	}

	actual, ok := c.Get("deploymentconfigs/test/deploy1")
	if e := scenario_1_deploymentConfig_imageSource_cacheEntry(); !ok || !reflect.DeepEqual(e, actual) {
		t.Fatalf("unexpected: %s\n%#v", diff.ObjectReflectDiff(e, actual), actual)
	}
	if err := verifyEntriesAt(c, []interface{}{scenario_1_deploymentConfig_imageSource_cacheEntry()}, "test/stream"); err != nil {
		t.Fatal(err)
	}

	// should have enqueued a single action (based on the image stream tag retriever)
	queued := queue.All()
	expected := []queuedOp{
		{key: "deploymentconfigs/test/deploy1", changes: []ImageChange{
			{Name: "stream:1", Tag: "1", Ref: "image/result:1", RV: 10, Namespace: "test", Trigger: empty},
			{Name: "stream:1", Tag: "1", Ref: "image/result:1", RV: 10, Namespace: "test", Trigger: empty},
		}},
	}
	if !queuedEqual(expected, queued) {
		t.Fatalf("changes: %#v", queued)
	}
}

func verifyEntriesAt(c cache.ThreadSafeStore, entries []interface{}, keys ...string) error {
	for _, key := range keys {
		indexed, err := c.ByIndex("images", key)
		if err != nil {
			return fmt.Errorf("unexpected error for key %s: %v", key, err)
		}
		if e, a := entries, indexed; !reflect.DeepEqual(e, a) {
			return fmt.Errorf("unexpected entry for key %s: %s", key, diff.ObjectReflectDiff(e, a))
		}
	}
	return nil
}

func scenario_1_buildConfig_strategy() *buildapi.BuildConfig {
	return &buildapi.BuildConfig{
		ObjectMeta: kapi.ObjectMeta{Name: "build1", Namespace: "test"},
		Spec: buildapi.BuildConfigSpec{
			Triggers: []buildapi.BuildTriggerPolicy{
				{ImageChange: &buildapi.ImageChangeTrigger{}},
			},
			CommonSpec: buildapi.CommonSpec{
				Strategy: buildapi.BuildStrategy{
					DockerStrategy: &buildapi.DockerBuildStrategy{
						From: &kapi.ObjectReference{Kind: "ImageStreamTag", Name: "stream:1"},
					},
				},
			},
		},
	}
}

func scenario_1_imageStream_single(namespace, name, rv string) *imageapi.ImageStream {
	return &imageapi.ImageStream{
		ObjectMeta: kapi.ObjectMeta{Name: name, Namespace: namespace, ResourceVersion: rv},
		Status: imageapi.ImageStreamStatus{
			Tags: map[string]imageapi.TagEventList{
				"1": {Items: []imageapi.TagEvent{
					{DockerImageReference: "image/result:1"},
				}},
			},
		},
	}
}

func scenario_1_buildConfig_strategy_cacheEntry() *TriggerCacheEntry {
	return &TriggerCacheEntry{
		Key:       "buildconfigs/test/build1",
		Namespace: "test",
		Triggers: []trigger.ObjectFieldTrigger{
			{From: trigger.ObjectReference{Kind: "ImageStreamTag", Name: "stream:1"}, FieldPath: "spec.strategy.*.from"},
		},
	}
}

func scenario_1_deploymentConfig_imageSource() *deployapi.DeploymentConfig {
	return &deployapi.DeploymentConfig{
		ObjectMeta: kapi.ObjectMeta{Name: "deploy1", Namespace: "test"},
		Spec: deployapi.DeploymentConfigSpec{
			Triggers: []deployapi.DeploymentTriggerPolicy{
				{ImageChangeParams: &deployapi.DeploymentTriggerImageChangeParams{
					Automatic:          true,
					ContainerNames:     []string{"first", "second"},
					From:               kapi.ObjectReference{Kind: "ImageStreamTag", Name: "stream:1"},
					LastTriggeredImage: "image/result:2",
				}},
				{ImageChangeParams: &deployapi.DeploymentTriggerImageChangeParams{
					Automatic:      true,
					ContainerNames: []string{"third"},
					From:           kapi.ObjectReference{Kind: "DockerImage", Name: "mysql", Namespace: "other"},
				}},
			},
			Template: &kapi.PodTemplateSpec{
				Spec: kapi.PodSpec{
					Containers: []kapi.Container{
						{Name: "first", Image: "image/result:2"},
						{Name: "second", Image: ""},
						{Name: "third", Image: ""},
					},
				},
			},
		},
	}
}

func scenario_1_deploymentConfig_imageSource_cacheEntry() *TriggerCacheEntry {
	return &TriggerCacheEntry{
		Key:       "deploymentconfigs/test/deploy1",
		Namespace: "test",
		Triggers: []trigger.ObjectFieldTrigger{
			{From: trigger.ObjectReference{Kind: "ImageStreamTag", Name: "stream:1"}, FieldPath: "spec.template.spec.containers[0].image"},
			{From: trigger.ObjectReference{Kind: "ImageStreamTag", Name: "stream:1"}, FieldPath: "spec.template.spec.containers[1].image"},
		},
	}
}

func scenario_1_buildConfig_imageSource() *buildapi.BuildConfig {
	return &buildapi.BuildConfig{
		ObjectMeta: kapi.ObjectMeta{Name: "build2", Namespace: "test2"},
		Spec: buildapi.BuildConfigSpec{
			Triggers: []buildapi.BuildTriggerPolicy{
				{ImageChange: &buildapi.ImageChangeTrigger{}},
				{ImageChange: &buildapi.ImageChangeTrigger{
					From:                 &kapi.ObjectReference{Kind: "ImageStreamTag", Name: "stream:2", Namespace: "other"},
					LastTriggeredImageID: "image/result:2",
				}},
				{ImageChange: &buildapi.ImageChangeTrigger{From: &kapi.ObjectReference{Kind: "DockerImage", Name: "mysql", Namespace: "other"}}},
			},
			CommonSpec: buildapi.CommonSpec{
				Strategy: buildapi.BuildStrategy{
					DockerStrategy: &buildapi.DockerBuildStrategy{
						From: &kapi.ObjectReference{Kind: "ImageStreamTag", Name: "stream:1"},
					},
				},
			},
		},
	}
}

func scenario_1_buildConfig_imageSource_cacheEntry() *TriggerCacheEntry {
	return &TriggerCacheEntry{
		Key:       "buildconfigs/test2/build2",
		Namespace: "test2",
		Triggers: []trigger.ObjectFieldTrigger{
			{From: trigger.ObjectReference{Kind: "ImageStreamTag", Name: "stream:1"}, FieldPath: "spec.strategy.*.from"},
			{From: trigger.ObjectReference{Kind: "ImageStreamTag", Name: "stream:2", Namespace: "other"}, FieldPath: "spec.triggers"},
		},
	}
}

func newFakeInformer(item, initialList runtime.Object) (cache.SharedInformer, *watch.FakeWatcher) {
	fw := watch.NewFake()
	lw := &cache.ListWatch{
		ListFunc: func(options kapi.ListOptions) (runtime.Object, error) {
			return initialList, nil
		},
		WatchFunc: func(options kapi.ListOptions) (watch.Interface, error) { return fw, nil },
	}
	informer := cache.NewSharedInformer(lw, item, 0)
	return informer, fw
}

type fakeImageReaction struct {
	lock    sync.Mutex
	nested  ImageReaction
	calls   int
	changes int
	err     error
}

type imageReactionFunc func(obj interface{}, changes []ImageChange) error

func (fn imageReactionFunc) ImageChanged(obj interface{}, changes []ImageChange) error {
	return fn(obj, changes)
}

func (r *fakeImageReaction) ImageChanged(obj interface{}, changes []ImageChange) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.nested != nil {
		r.nested.ImageChanged(obj, changes)
	}
	r.calls++
	r.changes += len(changes)
	return r.err
}

func (r *fakeImageReaction) Results() fakeImageReaction {
	r.lock.Lock()
	defer r.lock.Unlock()
	return *r
}

func randomStreamTag(r *rand.Rand, maxStreams, maxTags int32) string {
	return fmt.Sprintf("stream-%d:%d", r.Int31n(maxStreams), r.Int31n(maxTags))
}

func benchmark_1_buildConfig(r *rand.Rand, identity, maxStreams, maxTags, triggers int32) *buildapi.BuildConfig {
	bc := &buildapi.BuildConfig{
		ObjectMeta: kapi.ObjectMeta{Name: fmt.Sprintf("build-%d", identity), Namespace: "test"},
		Spec: buildapi.BuildConfigSpec{
			Triggers: []buildapi.BuildTriggerPolicy{
				{ImageChange: &buildapi.ImageChangeTrigger{}},
			},
			CommonSpec: buildapi.CommonSpec{
				Strategy: buildapi.BuildStrategy{
					DockerStrategy: &buildapi.DockerBuildStrategy{
						From: &kapi.ObjectReference{Kind: "ImageStreamTag", Name: randomStreamTag(r, maxStreams, maxTags)},
					},
				},
			},
		},
	}
	if triggers == 0 {
		bc.Spec.Triggers = nil
	}
	for i := int32(0); i < (triggers - 1); i++ {
		bc.Spec.Triggers = append(bc.Spec.Triggers, buildapi.BuildTriggerPolicy{
			ImageChange: &buildapi.ImageChangeTrigger{From: &kapi.ObjectReference{Kind: "ImageStreamTag", Name: randomStreamTag(r, maxStreams, maxTags)}},
		})
	}
	return bc
}

func benchmark_1_pod(r *rand.Rand, identity, maxStreams, maxTags, containers int32) *kapi.Pod {
	pod := &kapi.Pod{
		ObjectMeta: kapi.ObjectMeta{
			Name:      fmt.Sprintf("pod-%d", identity),
			Namespace: "test",
			Annotations: map[string]string{
				triggerAnnotationKey: fmt.Sprintf(
					`[
						{"from":{"kind":"ImageStreamTag","name":"%s"},"fieldPath":"spec.containers[0].image"},
						{"from":{"kind":"ImageStreamTag","name":"%s"},"fieldPath":"spec.containers[1].image"}
					]`,
					randomStreamTag(r, maxStreams, maxTags),
					randomStreamTag(r, maxStreams, maxTags),
				),
			},
		},
		Spec: kapi.PodSpec{},
	}
	for i := int32(0); i < containers; i++ {
		pod.Spec.Containers = append(pod.Spec.Containers, kapi.Container{Name: fmt.Sprintf("container-%d", i), Image: "initial-image"})
	}
	return pod
}

func benchmark_1_deploymentConfig(r *rand.Rand, identity, maxStreams, maxTags, containers int32) *deployapi.DeploymentConfig {
	dc := &deployapi.DeploymentConfig{
		ObjectMeta: kapi.ObjectMeta{
			Name:      fmt.Sprintf("dc-%d", identity),
			Namespace: "test",
		},
		Spec: deployapi.DeploymentConfigSpec{
			Template: &kapi.PodTemplateSpec{},
		},
	}
	for i := int32(0); i < containers; i++ {
		dc.Spec.Triggers = append(dc.Spec.Triggers, deployapi.DeploymentTriggerPolicy{
			ImageChangeParams: &deployapi.DeploymentTriggerImageChangeParams{
				Automatic:      true,
				ContainerNames: []string{fmt.Sprintf("container-%d", i)},
				From:           kapi.ObjectReference{Kind: "ImageStreamTag", Name: randomStreamTag(r, maxStreams, maxTags)},
			},
		})
		dc.Spec.Template.Spec.Containers = append(dc.Spec.Template.Spec.Containers, kapi.Container{Name: fmt.Sprintf("container-%d", i), Image: "initial-image"})
	}
	return dc
}

func benchmark_1_imageStream(identity, maxTags, sequence int32, round, index int) *imageapi.ImageStream {
	is := &imageapi.ImageStream{
		ObjectMeta: kapi.ObjectMeta{Name: fmt.Sprintf("stream-%d", identity), Namespace: "test"},
		Status:     imageapi.ImageStreamStatus{Tags: map[string]imageapi.TagEventList{}},
	}
	for i := int32(0); i < maxTags; i++ {
		is.Status.Tags[strconv.Itoa(int(i))] = imageapi.TagEventList{
			Items: []imageapi.TagEvent{
				{DockerImageReference: fmt.Sprintf("image-%d-%d:%d-%d-%d", identity, i, round, index, sequence)},
			},
		}
	}
	return is
}

func printTriggers(triggers []buildapi.BuildTriggerPolicy) string {
	var values []string
	for _, t := range triggers {
		if t.ImageChange.From != nil {
			values = append(values, fmt.Sprintf("[from=%s last=%s]", t.ImageChange.From.Name, t.ImageChange.LastTriggeredImageID))
		} else {
			values = append(values, fmt.Sprintf("[from=* last=%s]", t.ImageChange.LastTriggeredImageID))
		}
	}
	return strings.Join(values, ", ")
}

func printDeploymentTriggers(triggers []deployapi.DeploymentTriggerPolicy) string {
	var values []string
	for _, t := range triggers {
		values = append(values, fmt.Sprintf("[from=%s last=%s]", t.ImageChangeParams.From.Name, t.ImageChangeParams.LastTriggeredImage))
	}
	return strings.Join(values, ", ")
}

type bcHistoryRecorder struct {
	lock    sync.Mutex
	records map[string][]triggerRecord
}

func (r *bcHistoryRecorder) Record(changes []ImageChange, old, new *buildapi.BuildConfig) {
	previous := r.records[new.Namespace+"/"+new.Name]
	previous = append(previous, triggerRecord{
		oldRv:   old.ResourceVersion,
		rv:      new.ResourceVersion,
		changes: changes,
		old:     old.Spec.Triggers,
		new:     new.Spec.Triggers,
	})
	if len(previous) > 3 {
		previous = previous[len(previous)-3:]
	}
	r.records[new.Namespace+"/"+new.Name] = previous
}

func (r *bcHistoryRecorder) History(build *buildapi.BuildConfig) []triggerRecord {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.records[build.Namespace+"/"+build.Name]
}

type triggerRecord struct {
	oldRv, rv string
	old, new  []buildapi.BuildTriggerPolicy
	changes   []ImageChange
}

func (r triggerRecord) String() string {
	return fmt.Sprintf("\n@%s->%s{changes=%s}:%s\n", r.oldRv, r.rv, printOps(r.changes), diff.ObjectReflectDiff(r.old, r.new))
}

// alterBuildConfigFromTriggers will alter the incoming build config based on the trigger
// changes passed to it and send it back on the watch as a modification.
func alterBuildConfigFromTriggers(bcWatch *consistentWatch) imageReactionFunc {
	return imageReactionFunc(func(obj interface{}, changes []ImageChange) error {
		build := obj.(*buildapi.BuildConfig)
		var newBuild *buildapi.BuildConfig
		for _, change := range changes {
			if change.Trigger.FieldPath == "spec.strategy.*.from" {
				if build.Spec.Strategy.DockerStrategy.From.Name == change.Name {
					if build.Spec.Triggers[0].ImageChange.LastTriggeredImageID != change.Ref {
						if newBuild == nil {
							obj, _ = kapi.Scheme.DeepCopy(build)
							newBuild = obj.(*buildapi.BuildConfig)
						}
						newBuild.Spec.Triggers[0].ImageChange.LastTriggeredImageID = change.Ref
					}
					continue
				}
			} else {
				for i, t := range build.Spec.Triggers {
					if t.ImageChange.From.Name == change.Name {
						if build.Spec.Triggers[i].ImageChange.LastTriggeredImageID != change.Ref {
							if newBuild == nil {
								obj, _ = kapi.Scheme.DeepCopy(build)
								newBuild = obj.(*buildapi.BuildConfig)
							}
							newBuild.Spec.Triggers[i].ImageChange.LastTriggeredImageID = change.Ref
						}
						break
					}
				}
			}
		}
		if newBuild != nil {
			return bcWatch.Modify(newBuild)
		}
		return nil
	})
}

func alterDeploymentConfigFromTriggers(dcWatch *consistentWatch) imageReactionFunc {
	return imageReactionFunc(func(obj interface{}, changes []ImageChange) error {
		dc := obj.(*deployapi.DeploymentConfig)
		var newDC *deployapi.DeploymentConfig
		fn := func(i int, change ImageChange) {
			if dc.Spec.Template.Spec.Containers[i].Image != change.Ref {
				if newDC == nil {
					obj, _ = kapi.Scheme.DeepCopy(dc)
					newDC = obj.(*deployapi.DeploymentConfig)
				}
				newDC.Spec.Template.Spec.Containers[i].Image = change.Ref
				for _, trigger := range newDC.Spec.Triggers {
					if p := trigger.ImageChangeParams; p != nil && p.From.Name == change.Name && p.From.Kind == "ImageStreamTag" {
						p.LastTriggeredImage = change.Ref
					}
				}
			}
		}
		for _, change := range changes {
			if len(change.Ref) == 0 {
				continue
			}
			switch {
			case change.Trigger.FieldPath == "spec.template.spec.containers[0].image":
				fn(0, change)
			case change.Trigger.FieldPath == "spec.template.spec.containers[1].image":
				fn(1, change)
			default:
				panic(fmt.Errorf("unrecognized field path: %v", change.Trigger.FieldPath))
			}
		}
		if newDC != nil {
			return dcWatch.Modify(newDC)
		}
		return nil
	})
}

// alterPodFromTriggers will alter the incoming pod based on the trigger
// changes passed to it and send it back on the watch as a modification.
func alterPodFromTriggers(podWatch *watch.FakeWatcher) imageReactionFunc {
	count := 2
	return imageReactionFunc(func(obj interface{}, changes []ImageChange) error {
		pod := obj.(*kapi.Pod)
		var newPod *kapi.Pod
		fn := func(i int, change ImageChange) {
			if pod.Spec.Containers[i].Image != change.Ref {
				if newPod == nil {
					obj, _ = kapi.Scheme.DeepCopy(pod)
					newPod = obj.(*kapi.Pod)
				}
				newPod.Spec.Containers[i].Image = change.Ref
			}
		}
		for _, change := range changes {
			if len(change.Ref) == 0 {
				continue
			}
			switch {
			case change.Trigger.FieldPath == "spec.containers[0].image":
				fn(0, change)
			case change.Trigger.FieldPath == "spec.containers[1].image":
				fn(1, change)
			default:
				return fmt.Errorf("unrecognized field path: %v", change.Trigger.FieldPath)
			}
		}
		if newPod != nil {
			newPod.ResourceVersion = strconv.Itoa(count)
			count++
			podWatch.Modify(newPod)
		}
		return nil
	})
}

type consistentWatch struct {
	lock   sync.Mutex
	watch  *watch.FakeWatcher
	latest map[string]int64
}

func (w *consistentWatch) Add(obj runtime.Object) error {
	w.lock.Lock()
	defer w.lock.Unlock()
	m, err := meta.Accessor(obj)
	if err != nil {
		return err
	}
	if w.latest == nil {
		w.latest = make(map[string]int64)
	}
	if len(m.GetResourceVersion()) == 0 {
		m.SetResourceVersion("0")
	}
	rv, err := strconv.ParseInt(m.GetResourceVersion(), 10, 64)
	if err != nil {
		return err
	}
	key := m.GetNamespace() + "/" + m.GetName()
	if latest, ok := w.latest[key]; ok {
		if latest != rv {
			return kapierrs.NewAlreadyExists(unversioned.GroupResource{}, m.GetName())
		}
	}
	rv++
	w.latest[key] = rv
	m.SetResourceVersion(strconv.Itoa(int(rv)))
	w.watch.Add(obj)
	return nil
}

func (w *consistentWatch) Modify(obj runtime.Object) error {
	w.lock.Lock()
	defer w.lock.Unlock()
	m, err := meta.Accessor(obj)
	if err != nil {
		return err
	}
	if w.latest == nil {
		w.latest = make(map[string]int64)
	}
	if len(m.GetResourceVersion()) == 0 {
		m.SetResourceVersion("0")
	}
	rv, err := strconv.ParseInt(m.GetResourceVersion(), 10, 64)
	if err != nil {
		return err
	}
	key := m.GetNamespace() + "/" + m.GetName()
	if latest, ok := w.latest[key]; ok {
		if rv != 0 && latest != rv {
			return kapierrs.NewConflict(unversioned.GroupResource{}, m.GetName(), fmt.Errorf("unable to update, resource version %d does not match %d", rv, latest))
		}
	}
	rv++
	w.latest[key] = rv
	m.SetResourceVersion(strconv.Itoa(int(rv)))
	w.watch.Modify(obj)
	return nil
}

func TestTriggerController(t *testing.T) {
	// tuning
	var rounds, iterations = 100, 250
	var numStreams, numBuildConfigs, numPods, numDeploymentConfigs int32 = 10, 0, 0, 10
	var numTagsPerStream, maxTriggersPerBuild, maxContainersPerPod int32 = 5, 1, 2
	var ratioReferencedStreams, ratioTriggeredBuildConfigs float32 = 0.50, 1
	var ratioStreamChanges float32 = 0.50
	rnd := rand.New(rand.NewSource(1))

	stopCh := make(chan struct{})
	defer close(stopCh)
	bcInformer, bcFakeWatch := newFakeInformer(&buildapi.BuildConfig{}, &buildapi.BuildConfigList{ListMeta: unversioned.ListMeta{ResourceVersion: "1"}})
	bcWatch := &consistentWatch{watch: bcFakeWatch}
	isInformer, isFakeWatch := newFakeInformer(&imageapi.ImageStream{}, &imageapi.ImageStreamList{ListMeta: unversioned.ListMeta{ResourceVersion: "1"}})
	isWatch := &consistentWatch{watch: isFakeWatch}
	podInformer, podWatch := newFakeInformer(&kapi.Pod{}, &kapi.PodList{ListMeta: unversioned.ListMeta{ResourceVersion: "1"}})
	dcInformer, dcFakeWatch := newFakeInformer(&deployapi.DeploymentConfig{}, &deployapi.DeploymentConfigList{ListMeta: unversioned.ListMeta{ResourceVersion: "1"}})
	dcWatch := &consistentWatch{watch: dcFakeWatch}

	buildReactionFn := alterBuildConfigFromTriggers(bcWatch)
	buildReaction := &fakeImageReaction{nested: buildReactionFn}
	podReaction := &fakeImageReaction{nested: alterPodFromTriggers(podWatch)}
	deploymentReaction := &fakeImageReaction{nested: alterDeploymentConfigFromTriggers(dcWatch)}
	lister := StoreToImageStreamLister{isInformer.GetStore()}
	c := NewTriggerController(record.NewBroadcasterForTests(0), isInformer, lister,
		TriggerSource{
			Resource: unversioned.GroupResource{Resource: "buildconfigs"},
			Informer: bcInformer,
			TriggerFn: func(prefix string) TriggerIndexer {
				return buildConfigTriggerIndexer{prefix: prefix}
			},
			Reaction: buildReaction,
		},
		TriggerSource{
			Resource: unversioned.GroupResource{Resource: "deploymentconfigs"},
			Informer: dcInformer,
			TriggerFn: func(prefix string) TriggerIndexer {
				return deploymentConfigTriggerIndexer{prefix: prefix}
			},
			Reaction: deploymentReaction,
		},
		TriggerSource{
			Resource: unversioned.GroupResource{Resource: "pods"},
			Informer: podInformer,
			TriggerFn: func(prefix string) TriggerIndexer {
				return annotationTriggerIndexer{prefix: prefix}
			},
			Reaction: podReaction,
		},
	)
	isFn := c.syncImageStreamFn
	c.syncImageStreamFn = func(key string) error {
		if err := isFn(key); err != nil {
			t.Fatalf("failure on %s: %v", key, err)
		}
		return nil
	}
	resFn := c.syncResourceFn
	c.syncResourceFn = func(key string, changes []ImageChange) error {
		if err := resFn(key, changes); err != nil {
			t.Fatalf("failure on %s: %v", key, err)
		}
		return nil
	}
	go isInformer.Run(stopCh)
	go bcInformer.Run(stopCh)
	go podInformer.Run(stopCh)
	go dcInformer.Run(stopCh)
	go c.Run(1, stopCh)

	numReferencedStreams := int32(float32(numStreams) * ratioReferencedStreams)

	// generate an initial state
	for i := int32(0); i < numBuildConfigs; i++ {
		if i < int32(float32(numBuildConfigs)*ratioTriggeredBuildConfigs) {
			// builds that point to triggers
			if err := bcWatch.Add(benchmark_1_buildConfig(rnd, i, numReferencedStreams, numTagsPerStream, maxTriggersPerBuild)); err != nil {
				t.Fatal(err)
			}
		} else {
			// builds that have no image stream triggers
			if err := bcWatch.Add(benchmark_1_buildConfig(rnd, i, numStreams, numTagsPerStream, 0)); err != nil {
				t.Fatal(err)
			}
		}
	}
	for i := int32(0); i < numPods; i++ {
		// set initial pods
		podWatch.Add(benchmark_1_pod(rnd, i, numReferencedStreams, numTagsPerStream, maxContainersPerPod))
	}
	for i := int32(0); i < numDeploymentConfigs; i++ {
		// set initial deployments
		if err := dcWatch.Add(benchmark_1_deploymentConfig(rnd, i, numReferencedStreams, numTagsPerStream, maxContainersPerPod)); err != nil {
			t.Fatal(err)
		}
	}
	for i := int32(0); i < numStreams; i++ {
		// set initial image streams
		if err := isWatch.Add(benchmark_1_imageStream(i, numTagsPerStream, 1, 0, 0)); err != nil {
			t.Fatal(err)
		}
	}

	describe := map[string][]string{}

	// make a set of modifications to the streams or builds, verifying after each round
	for round := 1; round <= rounds; round++ {
		var changes []interface{}
		for i := 0; i < iterations; i++ {
			switch f := rnd.Float32(); {
			case f < ratioStreamChanges:
				streamNum := rnd.Int31n(numStreams)
				if err := isWatch.Modify(benchmark_1_imageStream(streamNum, numTagsPerStream, int32(2+(round-1)*500+i), round, i)); err != nil {
					t.Logf("[round=%d change=%d] failed to modify image stream: %v", round, i, err)
				}
			default:
				items := bcInformer.GetStore().List()
				if len(items) == 0 {
					continue
				}
				obj, _ := kapi.Scheme.DeepCopy(items[rnd.Int31n(int32(len(items)))])
				bc := obj.(*buildapi.BuildConfig)
				if len(bc.Spec.Triggers) > 0 {
					index := rnd.Int31n(int32(len(bc.Spec.Triggers)))
					trigger := &bc.Spec.Triggers[index]
					if trigger.ImageChange.From != nil {
						old := trigger.ImageChange.From.Name
						trigger.ImageChange.From.Name = randomStreamTag(rnd, numStreams, numTagsPerStream)
						describe[bc.Namespace+"/"+bc.Name] = append(describe[bc.Namespace+"/"+bc.Name], fmt.Sprintf("[round=%d change=%d]: change trigger %d from %q to %q", round, i, index, old, trigger.ImageChange.From.Name))
					} else {
						old := bc.Spec.Strategy.DockerStrategy.From.Name
						bc.Spec.Strategy.DockerStrategy.From.Name = randomStreamTag(rnd, numStreams, numTagsPerStream)
						describe[bc.Namespace+"/"+bc.Name] = append(describe[bc.Namespace+"/"+bc.Name], fmt.Sprintf("[round=%d change=%d]: change docker strategy from %q to %q", round, i, old, bc.Spec.Strategy.DockerStrategy.From.Name))
					}
					if err := bcWatch.Modify(bc); err != nil {
						t.Logf("[round=%d change=%d] failed to modify build config: %v", round, i, err)
					}
				}
			}
		}

		if !verifyState(c, t, changes, describe, bcInformer, podInformer, dcInformer) {
			t.Fatalf("halted after %d rounds", round)
		}
	}
}

func verifyState(
	c *TriggerController,
	t *testing.T,
	expected []interface{},
	descriptions map[string][]string,
	bcInformer, podInformer, dcInformer cache.SharedInformer,
) bool {

	if !controllerDrained(c) {
		t.Errorf("queue=%d changes=%d", c.queue.Len(), c.imageChangeQueue.Len())
		return false
	}

	// for _, item := range c.triggerCache.List() {
	// 	cached := item.(*TriggerCacheEntry)
	// 	t.Logf("cached: %#v", cached)
	// }

	// check that the operation queue is not leaking entries, just in case
	q := c.imageChangeQueue.(*imageChangeQueue)
	q.lock.Lock()
	if len(q.changes) != 0 {
		t.Fatalf("queued changes: %#v", q.changes)
	}
	q.lock.Unlock()

	failed := false
	times := 1

	// verify every build config points to the latest stream
	for i := 0; i < times; i++ {
		var failures []string
		for _, obj := range bcInformer.GetStore().List() {
			_, entry, changes, _, err := buildConfigTriggerIndexer{"buildconfigs/"}.Index(obj, nil)
			if err != nil {
				t.Fatalf("failed indexing: %v", err)
			}
			if len(changes) == 0 {
				continue
			}
			for _, change := range changes {
				if len(change.Ref) == 0 {
					failures = append(failures, fmt.Sprintf("%s did not resolve trigger on %s", entry.Key, change.Name))
					continue
				}
				ref, _, ok := c.tagRetriever.ImageStreamTag(change.Namespace, change.Name)
				if !ok || ref != change.Ref {
					streamName, _, _ := imageapi.SplitImageStreamTag(change.Name)
					indexed, _ := c.triggerCache.ByIndex("images", change.Namespace+"/"+streamName)
					var names []string
					for _, obj := range indexed {
						names = append(names, obj.(*TriggerCacheEntry).Key)
					}
					build := obj.(*buildapi.BuildConfig)
					failures = append(
						failures,
						fmt.Sprintf("%s(%s) did not resolve trigger on %s:\nhave: %s\nwant: %s\nlog:\n- %s\nindex: %v",
							entry.Key, build.ResourceVersion,
							change.Name,
							printTriggers(build.Spec.Triggers),
							ref,
							strings.Join(descriptions[build.Namespace+"/"+build.Name], "\n- "),
							names,
						),
					)
					continue
				}
			}
		}
		if len(failures) == 0 {
			break
		}
		if i == times-1 {
			sort.Strings(failures)
			for _, s := range failures {
				t.Errorf(s)
			}
			failed = true
		}
		time.Sleep(time.Millisecond)
	}

	// verify every deployment config points to the latest stream
	for i := 0; i < times; i++ {
		var failures []string
		for _, obj := range dcInformer.GetStore().List() {
			_, entry, changes, _, err := deploymentConfigTriggerIndexer{"deploymentconfigs/"}.Index(obj, nil)
			if err != nil {
				t.Fatalf("failed indexing: %v", err)
			}
			if len(changes) == 0 {
				continue
			}
			for _, change := range changes {
				if len(change.Ref) == 0 {
					failures = append(failures, fmt.Sprintf("%s did not resolve trigger on %s", entry.Key, change.Name))
					continue
				}
				ref, _, ok := c.tagRetriever.ImageStreamTag(change.Namespace, change.Name)
				if !ok || ref != change.Ref {
					streamName, _, _ := imageapi.SplitImageStreamTag(change.Name)
					indexed, _ := c.triggerCache.ByIndex("images", change.Namespace+"/"+streamName)
					var names []string
					for _, obj := range indexed {
						names = append(names, obj.(*TriggerCacheEntry).Key)
					}
					dc := obj.(*deployapi.DeploymentConfig)
					failures = append(
						failures,
						fmt.Sprintf("%s(%s) did not resolve trigger on %s:\nhave: %s\nwant: %s\nlog:\n- %s\nindex: %v",
							entry.Key, dc.ResourceVersion,
							change.Name,
							printDeploymentTriggers(dc.Spec.Triggers),
							ref,
							strings.Join(descriptions[dc.Namespace+"/"+dc.Name], "\n- "),
							names,
						),
					)
					continue
				}
			}
		}
		if len(failures) == 0 {
			break
		}
		if i == times-1 {
			sort.Strings(failures)
			for _, s := range failures {
				t.Errorf(s)
			}
			failed = true
		}
		time.Sleep(time.Millisecond)
	}

	// verify every pod points to the latest stream
	for i := 0; i < times; i++ {
		var failures []string
		for _, obj := range podInformer.GetStore().List() {
			_, entry, changes, _, err := annotationTriggerIndexer{"pods/"}.Index(obj, nil)
			if err != nil {
				t.Fatalf("failed indexing: %v", err)
			}
			if len(changes) == 0 {
				continue
			}
			for _, change := range changes {
				if len(change.Ref) == 0 {
					failures = append(failures, fmt.Sprintf("%s did not resolve trigger on %s", entry.Key, change.Name))
					continue
				}
				ref, _, ok := c.tagRetriever.ImageStreamTag(change.Namespace, change.Name)
				if !ok || ref != change.Ref {
					failures = append(failures, fmt.Sprintf("%s did not resolve trigger on %s - have %s, not %s", entry.Key, change.Name, change.Ref, ref))
					continue
				}
			}
		}
		if len(failures) == 0 {
			break
		}
		if i == times-1 {
			sort.Strings(failures)
			for _, s := range failures {
				t.Errorf(s)
			}
			failed = true
		}
		time.Sleep(time.Millisecond)
	}

	return !failed
}

func controllerDrained(c *TriggerController) bool {
	count := 0
	passed := 0
	for {
		if c.queue.Len() == 0 && c.imageChangeQueue.Len() == 0 {
			if passed > 5 {
				break
			}
			passed++
		} else {
			passed = 0
		}
		time.Sleep(time.Millisecond)
		count++
		if count > 3000 {
			return false
		}
	}
	return true
}
