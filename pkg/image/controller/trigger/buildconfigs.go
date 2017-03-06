package trigger

import (
	"fmt"
	"reflect"

	kapi "k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/client/cache"
	utilruntime "k8s.io/kubernetes/pkg/util/runtime"

	"github.com/golang/glog"
	buildapi "github.com/openshift/origin/pkg/build/api"
	buildutil "github.com/openshift/origin/pkg/build/util"
	imageapi "github.com/openshift/origin/pkg/image/api"
	"github.com/openshift/origin/pkg/image/api/v1/trigger"
)

// calculateBuildConfigTriggers transforms a build config into a set of image change triggers. If retrieveOperations
// is true an array of the latest state of the triggers will be returned.
func calculateBuildConfigTriggers(bc *buildapi.BuildConfig, retrieveOperations bool) ([]trigger.ObjectFieldTrigger, []ImageChange) {
	var triggers []trigger.ObjectFieldTrigger
	var changes []ImageChange
	for _, t := range bc.Spec.Triggers {
		if t.ImageChange == nil {
			continue
		}
		var (
			fieldPath string
			from      *kapi.ObjectReference
		)
		if t.ImageChange.From != nil {
			from = t.ImageChange.From
			fieldPath = "spec.triggers"
		} else {
			from = buildutil.GetInputReference(bc.Spec.Strategy)
			fieldPath = "spec.strategy.*.from"
		}
		if from == nil || from.Kind != "ImageStreamTag" || len(from.Name) == 0 {
			continue
		}

		// add a trigger
		triggers = append(triggers, trigger.ObjectFieldTrigger{
			From: trigger.ObjectReference{
				Name:       from.Name,
				Namespace:  from.Namespace,
				Kind:       from.Kind,
				APIVersion: from.APIVersion,
			},
			FieldPath: fieldPath,
		})

		// find the current trigger value as an operation if requested
		if !retrieveOperations {
			continue
		}
		namespace := from.Namespace
		if len(namespace) == 0 {
			namespace = bc.Namespace
		}
		_, tag, ok := imageapi.SplitImageStreamTag(from.Name)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("unable to trigger image change - tag reference %s on build config %s/%s not valid", from.Name, bc.Namespace, bc.Name))
			continue
		}
		changes = append(changes, ImageChange{
			Name:      from.Name,
			Tag:       tag,
			Ref:       t.ImageChange.LastTriggeredImageID,
			Namespace: namespace,
			Trigger:   &triggers[len(triggers)-1],
		})
	}
	return triggers, changes
}

// buildConfigTriggerIndexer converts build config events into entries for the trigger cache, and
// also calculates the latest state of the changes on the object.
type buildConfigTriggerIndexer struct {
	prefix string
}

func (i buildConfigTriggerIndexer) Index(obj, old interface{}) (string, *TriggerCacheEntry, []ImageChange, cache.DeltaType, error) {
	var (
		triggers   []trigger.ObjectFieldTrigger
		changes []ImageChange
		bc         *buildapi.BuildConfig
		change     cache.DeltaType
	)
	switch {
	case obj != nil && old == nil:
		// added
		bc = obj.(*buildapi.BuildConfig)
		triggers, changes = calculateBuildConfigTriggers(bc, true)
		change = cache.Added
	case old != nil && obj == nil:
		// deleted
		bc = old.(*buildapi.BuildConfig)
		triggers, _ = calculateBuildConfigTriggers(bc, false)
		change = cache.Deleted
	default:
		// updated
		bc = obj.(*buildapi.BuildConfig)
		triggers, changes = calculateBuildConfigTriggers(bc, true)
		oldTriggers, _ := calculateBuildConfigTriggers(old.(*buildapi.BuildConfig), false)
		switch {
		case len(oldTriggers) == 0:
			change = cache.Added
		case !reflect.DeepEqual(oldTriggers, triggers):
			change = cache.Updated
		}
	}

	if len(triggers) > 0 {
		key := i.prefix + bc.Namespace + "/" + bc.Name
		return key, &TriggerCacheEntry{
			Key:       key,
			Namespace: bc.Namespace,
			Triggers:  triggers,
		}, changes, change, nil
	}
	return "", nil, nil, change, nil
}

// BuildConfigInstantiator abstracts creating builds from build requests.
type BuildConfigInstantiator interface {
	// Instantiate should launch a build from the provided build request.
	Instantiate(namespace string, request *buildapi.BuildRequest) (*buildapi.Build, error)
}

// BuildConfigReaction converts trigger changes into new builds. It will request a build if
// at least one image is out of date.
type BuildConfigReaction struct {
	Instantiator BuildConfigInstantiator
}

// ImageChanged is passed a build config and a set of changes.
func (r *BuildConfigReaction) ImageChanged(obj interface{}, changes []ImageChange) error {
	bc := obj.(*buildapi.BuildConfig)
	var (
		changed bool
		id      string
		from    *kapi.ObjectReference
		ref     string
	)
	for _, t := range bc.Spec.Triggers {
		if t.ImageChange == nil {
			continue
		}
		for _, operation := range changes {
			if len(operation.Ref) == 0 {
				continue
			}
			switch operation.Trigger.FieldPath {
			case "spec.strategy.*.from":
				if t.ImageChange.From != nil {
					continue
				}
			case "spec.triggers":
				if t.ImageChange.From == nil || t.ImageChange.From.Name != operation.Name || t.ImageChange.From.Namespace != operation.Namespace {
					continue
				}
			}
			if t.ImageChange.LastTriggeredImageID != operation.Ref {
				changed = true
				//id = operation.ID
				ref = operation.Ref
				from = t.ImageChange.From
				if from == nil {
					from = buildutil.GetInputReference(bc.Spec.Strategy)
				}
				copied := *from
				from = &copied
			}
			break
		}
	}

	if !changed {
		return nil
	}

	// instantiate new build
	glog.V(4).Infof("Running build for BuildConfig %s/%s", bc.Namespace, bc.Name)
	request := &buildapi.BuildRequest{
		ObjectMeta: kapi.ObjectMeta{
			Name:      bc.Name,
			Namespace: bc.Namespace,
		},
		TriggeredBy: []buildapi.BuildTriggerCause{
			{
				Message: buildapi.BuildTriggerCauseImageMsg,
				ImageChangeBuild: &buildapi.ImageChangeCause{
					ImageID: id,
					FromRef: from,
				},
			},
		},
		TriggeredByImage: &kapi.ObjectReference{
			Kind: "DockerImage",
			Name: ref,
		},
		From: from,
	}
	_, err := r.Instantiator.Instantiate(bc.Namespace, request)
	return err
}
