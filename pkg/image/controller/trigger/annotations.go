package trigger

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	kapi "k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/api/meta"
	"k8s.io/kubernetes/pkg/client/cache"
	"k8s.io/kubernetes/pkg/runtime"

	ometa "github.com/openshift/origin/pkg/api/meta"
	imageapi "github.com/openshift/origin/pkg/image/api"
	"github.com/openshift/origin/pkg/image/api/v1/trigger"
)

const triggerAnnotationKey = "image.openshift.io/triggers"

func calculateAnnotationTriggers(obj interface{}, retrieveOperations bool, prefix string) (string, string, []trigger.ObjectFieldTrigger, []ImageChange, error) {
	m, err := meta.Accessor(obj)
	if err != nil {
		return "", "", nil, nil, err
	}
	var key, namespace string
	if namespace = m.GetNamespace(); len(namespace) > 0 {
		key = prefix + namespace + "/" + m.GetName()
	} else {
		key = prefix + m.GetName()
	}
	t, ok := m.GetAnnotations()[triggerAnnotationKey]
	if !ok {
		return key, namespace, nil, nil, nil
	}
	triggers := []trigger.ObjectFieldTrigger{}
	if err := json.Unmarshal([]byte(t), &triggers); err != nil {
		return key, namespace, nil, nil, err
	}
	if hasDuplicateTriggers(triggers) {
		return key, namespace, nil, nil, fmt.Errorf("duplicate triggers are not allowed")
	}
	var changes []ImageChange
	if len(triggers) > 0 && retrieveOperations {
		for i, trigger := range triggers {
			_, tag, ok := imageapi.SplitImageStreamTag(trigger.From.Name)
			if !ok {
				continue
			}
			ref, ok := extractRefFromObjectFieldPath(obj, trigger.FieldPath)
			if !ok {
				continue
			}
			changes = append(changes, ImageChange{
				Namespace: namespace,
				Name:      trigger.From.Name,
				Tag:       tag,
				Ref:       ref,
				Trigger:   &triggers[i],
			})
		}
	}
	return key, namespace, triggers, changes, nil
}

func hasDuplicateTriggers(triggers []trigger.ObjectFieldTrigger) bool {
	for i := range triggers {
		for j := i + 1; j < len(triggers); j++ {
			if triggers[i].FieldPath == triggers[j].FieldPath {
				return true
			}
		}
	}
	return false
}

func findPodSpecContainer(podSpec *kapi.PodSpec, path string) (*kapi.Container, string) {
	var arr []kapi.Container
	var remainder string
	switch {
	case strings.HasPrefix(path, "containers["):
		arr = podSpec.Containers
		remainder = strings.TrimPrefix(path, "containers[")
	case strings.HasPrefix(path, "initContainers["):
		arr = podSpec.InitContainers
		remainder = strings.TrimPrefix(path, "initContainers[")
	default:
		return nil, ""
	}
	end := strings.Index(remainder, "]")
	if end == -1 {
		return nil, ""
	}
	i, err := strconv.Atoi(remainder[:end])
	if err != nil {
		return nil, ""
	}
	remainder = remainder[end+1:]
	if len(remainder) > 0 && remainder[0] == '.' {
		remainder = remainder[1:]
	}
	if i >= len(arr) {
		return nil, ""
	}
	return &arr[i], remainder
}

func extractRefFromObjectFieldPath(obj interface{}, fieldPath string) (string, bool) {
	switch t := obj.(type) {
	case runtime.Object:
		spec, path, err := ometa.GetPodSpec(t)
		if err != nil {
			return "", false
		}
		relative := strings.TrimPrefix(fieldPath, path.String()+".")
		if relative == fieldPath {
			return "", false
		}
		switch {
		case strings.HasPrefix(relative, "containers[") && strings.HasSuffix(relative, "].image"):
			return containerImageByIndex(spec.Containers, strings.TrimSuffix(strings.TrimPrefix(relative, "containers["), "].image"))
		case strings.HasPrefix(relative, "initContainers[") && strings.HasSuffix(relative, "].image"):
			return containerImageByIndex(spec.InitContainers, strings.TrimSuffix(strings.TrimPrefix(relative, "initContainers["), "].image"))
		}
	}
	return "", false
}

func containerImageByIndex(containers []kapi.Container, index string) (string, bool) {
	if i, err := strconv.Atoi(index); err == nil {
		if i >= 0 && i < len(containers) {
			return containers[i].Image, true
		}
	}
	// TODO: potentially make this more flexible, like whitespace
	if name := strings.TrimSuffix(strings.TrimPrefix(index, "@name='"), "'"); name != index {
		for _, c := range containers {
			if c.Name == name {
				return c.Image, true
			}
		}
	}
	return "", false
}

// annotationTriggerIndexer uses annotations on objects to trigger changes.
type annotationTriggerIndexer struct {
	prefix string
}

func (i annotationTriggerIndexer) Index(obj, old interface{}) (string, *TriggerCacheEntry, []ImageChange, cache.DeltaType, error) {
	var (
		triggers   []trigger.ObjectFieldTrigger
		changes []ImageChange
		key        string
		namespace  string
		change     cache.DeltaType
		err        error
	)
	switch {
	case obj != nil && old == nil:
		// added
		key, namespace, triggers, changes, err = calculateAnnotationTriggers(obj, true, i.prefix)
		if err != nil {
			return "", nil, nil, change, err
		}
		change = cache.Added
	case old != nil && obj == nil:
		// deleted
		key, namespace, triggers, _, err = calculateAnnotationTriggers(old, false, i.prefix)
		if err != nil {
			return "", nil, nil, change, err
		}
		change = cache.Deleted
	default:
		// updated
		key, namespace, triggers, changes, err = calculateAnnotationTriggers(obj, true, i.prefix)
		if err != nil {
			return "", nil, nil, change, err
		}
		_, _, _, oldTriggers, err := calculateAnnotationTriggers(old, false, i.prefix)
		if err != nil {
			return "", nil, nil, change, err
		}
		switch {
		case len(oldTriggers) == 0:
			change = cache.Added
		case !reflect.DeepEqual(oldTriggers, triggers):
			change = cache.Updated
		}
	}

	if len(triggers) > 0 {
		return key, &TriggerCacheEntry{
			Key:       key,
			Namespace: namespace,
			Triggers:  triggers,
		}, changes, change, nil
	}
	return "", nil, nil, change, nil
}
