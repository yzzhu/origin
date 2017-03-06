package trigger

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"

	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/api/errors"
	"k8s.io/kubernetes/pkg/api/unversioned"
	"k8s.io/kubernetes/pkg/client/cache"
	kcoreclient "k8s.io/kubernetes/pkg/client/clientset_generated/internalclientset/typed/core/internalversion"
	"k8s.io/kubernetes/pkg/client/record"
	"k8s.io/kubernetes/pkg/controller"
	utilruntime "k8s.io/kubernetes/pkg/util/runtime"
	"k8s.io/kubernetes/pkg/util/wait"
	"k8s.io/kubernetes/pkg/util/workqueue"

	imageapi "github.com/openshift/origin/pkg/image/api"
	"math"
)

const (
	// maxRetries is the number of times an image stream will be retried before it is dropped out of the queue.
	maxRetries          = 5
	maxResourceInterval = 30 * time.Second
)

// ErrUnresolvedTag is used to indicate a resource is not ready to be triggered
var ErrUnresolvedTag = fmt.Errorf("one or more triggers on this object cannot be resolved")

type ImageReaction interface {
	ImageChanged(obj interface{}, changes []ImageChange) error
}

// TagRetriever returns information about a tag, including whether it exists
// and the observed resource version of the object at the time the tag was loaded.
type TagRetriever interface {
	ImageStreamTag(namespace, name string) (ref string, rv int64, ok bool)
}

// ImageStreamNamespaceLister helps get ImageStreams.
type ImageStreamNamespaceLister interface {
	// Get retrieves the Deployment from the indexer for a given namespace and name.
	Get(name string) (*imageapi.ImageStream, error)
}

type ImageStreamLister interface {
	// ImageStreams returns an object that can get ImageStreams.
	ImageStreams(namespace string) ImageStreamNamespaceLister
}

type TriggerSource struct {
	Informer  cache.SharedInformer
	Store     cache.Store
	Resource  unversioned.GroupResource
	TriggerFn func(prefix string) TriggerIndexer
	Reaction  ImageReaction
}

type tagRetriever struct {
	lister ImageStreamLister
}

func (r tagRetriever) ImageStreamTag(namespace, name string) (ref string, rv int64, ok bool) {
	streamName, tag, ok := imageapi.SplitImageStreamTag(name)
	if !ok {
		return "", 0, false
	}
	is, err := r.lister.ImageStreams(namespace).Get(streamName)
	if err != nil {
		return "", 0, false
	}
	rv, err = strconv.ParseInt(is.ResourceVersion, 10, 64)
	if err != nil {
		return "", 0, false
	}
	ref, ok = imageapi.ResolveLatestTaggedImage(is, tag)
	return ref, rv, ok
}

// StoreToImageStreamLister adapts a cache.Store to ImageStreamLister
type StoreToImageStreamLister struct {
	cache.Store
}

func (s StoreToImageStreamLister) ImageStreams(namespace string) ImageStreamNamespaceLister {
	return storeImageStreamsNamespacer{s.Store, namespace}
}

type storeImageStreamsNamespacer struct {
	store     cache.Store
	namespace string
}

// Get the image stream matching the name from the cache.
func (s storeImageStreamsNamespacer) Get(name string) (*imageapi.ImageStream, error) {
	obj, exists, err := s.store.GetByKey(s.namespace + "/" + name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.NewNotFound(imageapi.Resource("imagestream"), name)
	}
	return obj.(*imageapi.ImageStream), nil
}

func defaultResourceFailureDelay(requeue int) time.Duration {
	if requeue > 5 {
		return maxResourceInterval
	}
	t := time.Duration(math.Pow(2.0, float64(requeue)) * float64(time.Second))
	if t > maxResourceInterval {
		t = maxResourceInterval
	}
	return t
}

type TriggerController struct {
	eventRecorder record.EventRecorder

	triggerCache   cache.ThreadSafeStore
	triggerSources map[string]TriggerSource

	// To allow injection of syncs for testing.
	syncImageStreamFn func(key string) error
	// To allow injection of syncs for testing.
	syncResourceFn func(key string, changes []ImageChange) error
	// used for unit testing
	enqueueImageStreamFn func(is *imageapi.ImageStream)
	// Allows injection for testing, controls requeues on image errors
	resourceFailureDelayFn func(requeue int) time.Duration

	// lister can list/get image streams from the shared informer's store
	lister ImageStreamLister
	// tagRetriever helps get the latest value of a tag
	tagRetriever TagRetriever

	// queue is the list of image stream keys that must be synced.
	queue workqueue.RateLimitingInterface
	// imageChangeQueue tracks the pending changes to objects
	imageChangeQueue ImageChangeQueue

	// syncs are the items that must return true before the queue can be processed
	syncs []cache.InformerSynced
}

func NewTriggerEventBroadcaster(client kcoreclient.CoreInterface) record.EventBroadcaster {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(glog.Infof)
	// TODO: remove the wrapper when every client has moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&kcoreclient.EventSinkImpl{Interface: client.Events("")})
	return eventBroadcaster
}

func NewTriggerController(eventBroadcaster record.EventBroadcaster, isInformer cache.SharedInformer, lister ImageStreamLister, sources ...TriggerSource) *TriggerController {
	c := &TriggerController{
		eventRecorder:  eventBroadcaster.NewRecorder(api.EventSource{Component: "image-trigger-controller"}),
		queue:          workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "image-trigger"),
		imageChangeQueue: newOperationQueue(workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "image-trigger-reactions")),
		lister:         lister,
		tagRetriever:   tagRetriever{lister},
		triggerCache:   NewTriggerCache(),

		resourceFailureDelayFn: defaultResourceFailureDelay,
	}

	c.syncImageStreamFn = c.syncImageStream
	c.syncResourceFn = c.syncResource
	c.enqueueImageStreamFn = c.enqueueImageStream

	isInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.addImageStreamNotification,
		UpdateFunc: c.updateImageStreamNotification,
	})
	c.syncs = []cache.InformerSynced{isInformer.HasSynced}

	triggers, syncs, err := setupTriggerSources(c.triggerCache, c.tagRetriever, sources, c.imageChangeQueue)
	if err != nil {
		panic(err)
	}
	c.triggerSources = triggers
	c.syncs = append(c.syncs, syncs...)

	return c
}

func setupTriggerSources(triggerCache cache.ThreadSafeStore, tagRetriever TagRetriever, sources []TriggerSource, imageChangeQueue ImageChangeQueue) (map[string]TriggerSource, []cache.InformerSynced, error) {
	var syncs []cache.InformerSynced
	triggerSources := make(map[string]TriggerSource)
	for _, source := range sources {
		if source.Store == nil {
			source.Store = source.Informer.GetStore()
		}
		prefix := source.Resource.String() + "/"
		if _, ok := triggerSources[source.Resource.String()]; ok {
			return nil, nil, fmt.Errorf("duplicate resource names registered in %#v", sources)
		}
		triggerSources[source.Resource.String()] = source

		handler := ProcessEvents(triggerCache, source.TriggerFn(prefix), imageChangeQueue, tagRetriever)
		source.Informer.AddEventHandler(handler)
		syncs = append(syncs, source.Informer.HasSynced)
	}
	return triggerSources, syncs, nil
}

// Run begins watching and syncing.
func (c *TriggerController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	glog.Infof("Starting trigger controller")

	if !cache.WaitForCacheSync(stopCh, c.syncs...) {
		utilruntime.HandleError(fmt.Errorf("timed out waiting for caches to sync"))
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(c.imageStreamWorker, time.Second, stopCh)
	}

	for i := 0; i < workers; i++ {
		go wait.Until(c.resourceWorker, time.Second, stopCh)
	}

	<-stopCh
	glog.Infof("Shutting down trigger controller")
}

func (c *TriggerController) addImageStreamNotification(obj interface{}) {
	is := obj.(*imageapi.ImageStream)
	//glog.V(4).Infof("Adding image stream %s", is.Name)
	c.enqueueImageStreamFn(is)
}

func (c *TriggerController) updateImageStreamNotification(old, cur interface{}) {
	//oldIS := old.(*imageapi.ImageStream)
	//glog.V(4).Infof("Updating image stream %s %s", oldIS.Name, cur.(*imageapi.ImageStream).ResourceVersion)
	c.enqueueImageStreamFn(cur.(*imageapi.ImageStream))
}

func (c *TriggerController) enqueueImageStream(is *imageapi.ImageStream) {
	key, err := controller.KeyFunc(is)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get key for object %#v: %v", is, err))
		return
	}
	c.queue.Add(key)
}

func (c *TriggerController) enqueueRateLimited(is *imageapi.ImageStream) {
	key, err := controller.KeyFunc(is)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get key for object %#v: %v", is, err))
		return
	}
	c.queue.AddRateLimited(key)
}

// enqueueAfter will enqueue an image stream after the provided amount of time.
func (c *TriggerController) enqueueAfter(is *imageapi.ImageStream, after time.Duration) {
	key, err := controller.KeyFunc(is)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get key for object %#v: %v", is, err))
		return
	}
	c.queue.AddAfter(key, after)
}

// imageStreamWorker runs a worker thread that just dequeues items, processes them, and marks them done.
// It enforces that the syncHandler is never invoked concurrently with the same key.
func (c *TriggerController) imageStreamWorker() {
	for c.processNextImageStream() {
	}
	glog.V(4).Infof("Image stream worker stopped")
}

func (c *TriggerController) processNextImageStream() bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(key)

	err := c.syncImageStreamFn(key.(string))
	c.handleImageStreamErr(err, key)

	return true
}

func (c *TriggerController) handleImageStreamErr(err error, key interface{}) {
	if err == nil {
		c.queue.Forget(key)
		return
	}

	if c.queue.NumRequeues(key) < maxRetries {
		glog.V(4).Infof("Error syncing image stream %v: %v", key, err)
		c.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	glog.V(4).Infof("Dropping image stream %q out of the queue: %v", key, err)
	c.queue.Forget(key)
}

// resourceWorker runs a worker thread that just dequeues items, processes them, and marks them done.
// It enforces that the syncHandler is never invoked concurrently with the same key.
func (c *TriggerController) resourceWorker() {
	for c.processNextResource() {
	}
	glog.V(4).Infof("Resource worker stopped")
}

func (c *TriggerController) processNextResource() bool {
	key, changes, quit := c.imageChangeQueue.Get()
	if quit {
		return false
	}
	defer c.imageChangeQueue.Done(key)

	err := c.syncResourceFn(key, changes)
	c.handleResourceErr(err, key, changes)

	return true
}

func (c *TriggerController) handleResourceErr(err error, key string, changes []ImageChange) {
	if err == nil {
		c.imageChangeQueue.Forget(key)
		return
	}

	if delay := c.resourceFailureDelayFn(c.imageChangeQueue.NumRequeues(key)); delay > 0 {
		glog.V(4).Infof("Error syncing resource %v: %v", key, err)
		c.imageChangeQueue.AddAfter(key, changes, delay)
		return
	}

	utilruntime.HandleError(err)
	glog.V(4).Infof("Dropping resource %q out of the queue: %v", key, err)
	c.imageChangeQueue.Forget(key)
}

// syncImageStream will sync the image stream with the given key.
// This function is not meant to be invoked concurrently with the same key.
func (c *TriggerController) syncImageStream(key string) error {
	startTime := time.Now()
	glog.V(4).Infof("Started syncing image stream %q (%v)", key, startTime)
	defer func() {
		glog.V(4).Infof("Finished syncing image stream %q (%v)", key, time.Now().Sub(startTime))
	}()

	// find the set of triggers to act on
	triggered, err := c.triggerCache.ByIndex("images", key)
	if err != nil {
		return err
	}
	if len(triggered) == 0 {
		return nil
	}

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	is, err := c.lister.ImageStreams(namespace).Get(name)
	if errors.IsNotFound(err) {
		glog.V(4).Infof("Image stream %v has been deleted", key)
		return nil
	}
	if err != nil {
		return fmt.Errorf("unable to retrieve image stream %v from store: %v", key, err)
		return err
	}

	rv, err := strconv.ParseInt(is.ResourceVersion, 10, 64)
	if err != nil {
		return fmt.Errorf("image stream %s/%s resource version is not parseable: %s: %v", namespace, name, is.ResourceVersion, err)
	}

	// queue an operation for every trigger
	for tag, v := range is.Status.Tags {
		if len(v.Items) == 0 {
			continue
		}
		ref, ok := imageapi.ResolveTagReference(is, tag, &v.Items[0])
		if !ok {
			continue
		}
		name := imageapi.JoinImageStreamTag(name, tag)
		for _, t := range triggered {
			entry := t.(*TriggerCacheEntry)
			if changes := createTriggerOperationsForEntry(entry, name, tag, ref, namespace, rv); len(changes) > 0 {
				c.imageChangeQueue.Add(entry.Key, changes)
			}
		}
	}
	return nil
}

// syncResource handles a set of changes against one of the possible resources generically.
func (c *TriggerController) syncResource(key string, changes []ImageChange) error {
	if len(changes) == 0 {
		return nil
	}
	parts := strings.SplitN(key, "/", 2)
	source := c.triggerSources[parts[0]]
	obj, exists, err := source.Store.GetByKey(parts[1])
	if err != nil {
		return fmt.Errorf("unable to retrieve %s %s from store: %v", parts[0], parts[1], err)
	}
	if !exists {
		return nil
	}

	// For any trigger changes that haven't shown up yet, resolve them. If a failure occurs, we'll
	// rate limit that operation.
	for i, change := range changes {
		if len(change.Ref) == 0 {
			ref, rv, ok := c.tagRetriever.ImageStreamTag(change.Namespace, change.Name)
			if !ok {
				continue
			}
			changes[i].Ref, changes[i].RV = ref, rv
		}
	}

	if err := source.Reaction.ImageChanged(obj, changes); err != nil {
		return fmt.Errorf("unable to react to new image for %s %s from store: %v", parts[0], parts[1], err)
	}
	return nil
}
