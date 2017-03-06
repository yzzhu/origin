package trigger

import (
	"fmt"

	"k8s.io/kubernetes/pkg/client/cache"
	utilruntime "k8s.io/kubernetes/pkg/util/runtime"

	imageapi "github.com/openshift/origin/pkg/image/api"
	"github.com/openshift/origin/pkg/image/api/v1/trigger"
)

func NewTriggerCache() cache.ThreadSafeStore {
	return cache.NewThreadSafeStore(
		cache.Indexers{
			"images": func(obj interface{}) ([]string, error) {
				entry := obj.(*TriggerCacheEntry)
				var keys []string
				for _, t := range entry.Triggers {
					if t.From.Kind != "ImageStreamTag" || len(t.From.APIVersion) != 0 || t.Paused {
						continue
					}
					name, _, ok := imageapi.SplitImageStreamTag(t.From.Name)
					if !ok {
						continue
					}
					namespace := t.From.Namespace
					if len(namespace) == 0 {
						namespace = entry.Namespace
					}
					keys = append(keys, namespace+"/"+name)
				}
				return keys, nil
			},
		},
		cache.Indices{},
	)
}

type TriggerCacheEntry struct {
	Key       string
	Namespace string
	Triggers  []trigger.ObjectFieldTrigger
}

func TriggerCacheEntryEqual(a, b interface{}) bool {
	entryA, entryB := a.(*TriggerCacheEntry), b.(*TriggerCacheEntry)
	if entryA.Key != entryB.Key {
		return false
	}
	if len(entryA.Triggers) != len(entryB.Triggers) {
		return false
	}
	for i, tA := range entryA.Triggers {
		tB := entryB.Triggers[i]
		if tA.From != tB.From || tA.FieldPath != tB.FieldPath || tA.Paused != tB.Paused {
			return false
		}
	}
	return true
}

type TriggerIndexer interface {
	// Index takes the given pair of objects and turns it into a key and a value. The returned key
	// will be used to store the object. obj is set on adds and updates, old is set on deletes and updates.
	// Changed should be true if the triggers changed.  Operations is a list of actions that should be sent
	// to the reaction.
	Index(obj, old interface{}) (key string, entry *TriggerCacheEntry, changes []ImageChange, change cache.DeltaType, err error)
}

// queueChangedTriggers adds any changes to the queue that have a changed image ref or the tag does not exist.
// This prevents races between image streams and the triggered object resulting in no tags being fired - instead,
// the operation queue will retry the changes until the limit is reached.
func queueChangedTriggers(queue ImageChangeQueue, key string, changes []ImageChange, tags TagRetriever) {
	if len(changes) == 0 {
		return
	}
	var changed []ImageChange
	for _, change := range changes {
		ref, rv, ok := tags.ImageStreamTag(change.Namespace, change.Name)
		switch {
		case !ok && len(change.Ref) == 0:
			// tag doesn't exist and we've never been queued, so throw the change into the queue and check again
			changed = append(changed, change)
		case !ok:
			// we're in the index, but tag may have been deleted or we are racing against the change, so we don't
			// have to do anything
		case len(ref) > 0 && ref != change.Ref:
			// tag is out of date
			change.RV = rv
			change.Ref = ref
			changed = append(changed, change)
		}
	}
	if len(changed) == 0 {
		return
	}
	queue.Add(key, changed)
}

// ProcessEvents returns a ResourceEventHandler suitable for use with an Informer to maintain the cache.
// indexer is responsible for calculating triggers and any pending changes. Operations are added to
// the operation queue if a change is required.
func ProcessEvents(c cache.ThreadSafeStore, indexer TriggerIndexer, queue ImageChangeQueue, tags TagRetriever) cache.ResourceEventHandler {
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			key, entry, changes, _, err := indexer.Index(obj, nil)
			if err != nil {
				utilruntime.HandleError(fmt.Errorf("unable to extract cache data from %T: %v", obj, err))
				return
			}
			if entry != nil {
				c.Add(key, entry)
				queueChangedTriggers(queue, key, changes, tags)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			key, entry, changes, change, err := indexer.Index(newObj, oldObj)
			if err != nil {
				utilruntime.HandleError(fmt.Errorf("unable to extract cache data from %T: %v", newObj, err))
				return
			}
			switch {
			case entry == nil:
				c.Delete(key)
			case change == cache.Added:
				c.Add(key, entry)
				queueChangedTriggers(queue, key, changes, tags)
			case change == cache.Updated:
				c.Update(key, entry)
				queueChangedTriggers(queue, key, changes, tags)
			}
		},
		DeleteFunc: func(obj interface{}) {
			key, entry, _, _, err := indexer.Index(nil, obj)
			if err != nil {
				utilruntime.HandleError(fmt.Errorf("unable to extract cache data from %T: %v", obj, err))
				return
			}
			if entry != nil {
				c.Delete(key)
			}
		},
	}
}
