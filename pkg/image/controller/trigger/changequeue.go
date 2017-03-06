package trigger

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"k8s.io/kubernetes/pkg/util/workqueue"

	"github.com/openshift/origin/pkg/image/api/v1/trigger"
)

// ImageChange is a record of a pending image change to a field on an object
// controlled by a trigger.
type ImageChange struct {
	// Name is the name of the image stream tag this change originated with.
	Name string
	// Tag is the extracted tag value from the Name field.
	Tag string
	// Ref is the value the image will change to (this is a DockerImage reference)
	Ref string
	// Namespace is the namespace the image stream is located in.
	Namespace string
	// RV is the resource version of the image stream resource that triggered the change.
	RV int64

	// The trigger that resulted in this change.
	Trigger *trigger.ObjectFieldTrigger
}

func printTriggerNames(triggers []trigger.ObjectFieldTrigger) string {
	var names []string
	for _, t := range triggers {
		names = append(names, t.From.Name)
	}
	return strings.Join(names, ",")
}

func printOps(changes []ImageChange) string {
	var out []string
	for _, change := range changes {
		out = append(out, fmt.Sprintf("%s->%s(%d)", change.Name, change.Ref, change.RV))
	}
	return strings.Join(out, ",")
}

// createTriggerOperationsForEntry turns a trigger cache entry into an array of TriggerOperations
func createTriggerOperationsForEntry(entry *TriggerCacheEntry, name, tag, ref, namespace string, rv int64) []ImageChange {
	var changes []ImageChange
	for i, trigger := range entry.Triggers {
		if trigger.From.Name != name || trigger.From.Kind != "ImageStreamTag" || trigger.From.APIVersion != "" || trigger.Paused {
			continue
		}
		changes = append(changes, ImageChange{
			Name:      name,
			Tag:       tag,
			Ref:       ref,
			RV:        rv,
			Namespace: namespace,
			Trigger:   &entry.Triggers[i],
		})
	}
	return changes
}

// ImageChangeQueue tracks the pending image changes for resources. Allows work to
// be sharded and aggregated by resource.
type ImageChangeQueue interface {
	Add(key string, changes []ImageChange)
	AddRateLimited(key string, changes []ImageChange)
	AddAfter(key string, changes []ImageChange, duration time.Duration)
	NumRequeues(key string) int
	// Get returns the next key off the queue.
	Get() (key string, opts []ImageChange, shutdown bool)
	Done(key string)
	Forget(key string)
	Len() int
}

// newOperationQueue creates a new changes queue from a workqueue.
func newOperationQueue(queue workqueue.RateLimitingInterface) ImageChangeQueue {
	return &imageChangeQueue{
		queue:   queue,
		changes: make(map[string][]ImageChange),
	}
}

type imageChangeQueue struct {
	lock    sync.Mutex
	queue   workqueue.RateLimitingInterface
	changes map[string][]ImageChange
}

func (q *imageChangeQueue) Add(key string, newOps []ImageChange) {
	q.lock.Lock()
	defer q.lock.Unlock()
	changes, ok := q.changes[key]
	if ok {
		q.changes[key] = mergeOperations(changes, newOps)
	} else {
		q.changes[key] = newOps
	}

	q.queue.Add(key)
}

func (q *imageChangeQueue) AddRateLimited(key string, newOps []ImageChange) {
	q.lock.Lock()
	defer q.lock.Unlock()
	changes, ok := q.changes[key]
	if ok {
		q.changes[key] = mergeOperations(changes, newOps)
	} else {
		q.changes[key] = newOps
	}

	q.queue.AddRateLimited(key)
}

func (q *imageChangeQueue) AddAfter(key string, newOps []ImageChange, duration time.Duration) {
	q.lock.Lock()
	defer q.lock.Unlock()
	changes, ok := q.changes[key]
	if ok {
		q.changes[key] = mergeOperations(changes, newOps)
	} else {
		q.changes[key] = newOps
	}

	q.queue.AddAfter(key, duration)
}

func (q *imageChangeQueue) Get() (string, []ImageChange, bool) {
	item, shutdown := q.queue.Get()

	q.lock.Lock()
	defer q.lock.Unlock()
	key := item.(string)
	changes := q.changes[key]
	delete(q.changes, key)
	return key, changes, shutdown
}

func (q *imageChangeQueue) NumRequeues(key string) int {
	return q.queue.NumRequeues(key)
}

func (q *imageChangeQueue) Done(key string) {
	q.queue.Done(key)
}

func (q *imageChangeQueue) Forget(key string) {
	q.queue.Forget(key)
}

func (q *imageChangeQueue) Len() int {
	return q.queue.Len()
}

// mergeOperations ensures that changes that target the same field path are merged.
func mergeOperations(existing, changes []ImageChange) []ImageChange {
	for _, change := range changes {
		if mergeOperation(existing, change) {
			continue
		}
		existing = append(existing, change)
	}
	return existing
}

// mergeOperation finds any equivalent changes in existing to change and updates
// existing in place. It will return true if an equivalent was found, or false
// if the operation is new.
func mergeOperation(changes []ImageChange, change ImageChange) bool {
	for i, existing := range changes {
		if change.Trigger.FieldPath != existing.Trigger.FieldPath {
			continue
		}
		if change.Name == existing.Name && change.Namespace == existing.Namespace && change.RV < existing.RV {
			return true
		}
		changes[i] = change
		return true
	}
	return false
}
