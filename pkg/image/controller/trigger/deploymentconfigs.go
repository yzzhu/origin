package trigger

import (
	"fmt"
	"reflect"

	kapi "k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/client/cache"
	utilruntime "k8s.io/kubernetes/pkg/util/runtime"

	"github.com/openshift/origin/pkg/client"
	deployapi "github.com/openshift/origin/pkg/deploy/api"
	imageapi "github.com/openshift/origin/pkg/image/api"
	"github.com/openshift/origin/pkg/image/api/v1/trigger"
	"strings"
)

func indicesForContainerNames(containers []kapi.Container, names []string) []int {
	var index []int
	for _, name := range names {
		for i, container := range containers {
			if name == container.Name {
				index = append(index, i)
			}
		}
	}
	return index
}

func namesInclude(names []string, name string) bool {
	for _, n := range names {
		if name == n {
			return true
		}
	}
	return false
}

// calculateDeploymentConfigTriggers transforms a build config into a set of image change triggers. If retrieveOperations
// is true an array of the latest state of the triggers will be returned.
func calculateDeploymentConfigTriggers(dc *deployapi.DeploymentConfig, retrieveOperations bool) ([]trigger.ObjectFieldTrigger, []ImageChange) {
	var triggers []trigger.ObjectFieldTrigger
	var changes []ImageChange
	for _, t := range dc.Spec.Triggers {
		if t.ImageChangeParams == nil {
			continue
		}
		from := t.ImageChangeParams.From
		if from.Kind != "ImageStreamTag" || len(from.Name) == 0 {
			continue
		}

		// find the current trigger value as an operation if requested
		var operation ImageChange
		if retrieveOperations {
			namespace := from.Namespace
			if len(namespace) == 0 {
				namespace = dc.Namespace
			}
			_, tag, ok := imageapi.SplitImageStreamTag(from.Name)
			if !ok {
				utilruntime.HandleError(fmt.Errorf("unable to trigger image change - tag reference %s on deployment config %s/%s not valid", from.Name, dc.Namespace, dc.Name))
				continue
			}
			operation = ImageChange{
				Name:      from.Name,
				Tag:       tag,
				Ref:       t.ImageChangeParams.LastTriggeredImage,
				Namespace: namespace,
			}
		}

		// add one trigger per init container and container
		for _, index := range indicesForContainerNames(dc.Spec.Template.Spec.Containers, t.ImageChangeParams.ContainerNames) {
			fieldPath := fmt.Sprintf("spec.template.spec.containers[%d].image", index)
			triggers = append(triggers, trigger.ObjectFieldTrigger{
				From: trigger.ObjectReference{
					Name:       from.Name,
					Namespace:  from.Namespace,
					Kind:       from.Kind,
					APIVersion: from.APIVersion,
				},
				FieldPath: fieldPath,
				Paused:    !t.ImageChangeParams.Automatic,
			})
			if retrieveOperations {
				operation.Trigger = &triggers[len(triggers)-1]
				changes = append(changes, operation)
			}
		}
		for _, index := range indicesForContainerNames(dc.Spec.Template.Spec.InitContainers, t.ImageChangeParams.ContainerNames) {
			fieldPath := fmt.Sprintf("spec.template.spec.initContainers[%d].image", index)
			triggers = append(triggers, trigger.ObjectFieldTrigger{
				From: trigger.ObjectReference{
					Name:       from.Name,
					Namespace:  from.Namespace,
					Kind:       from.Kind,
					APIVersion: from.APIVersion,
				},
				FieldPath: fieldPath,
				Paused:    !t.ImageChangeParams.Automatic,
			})
			if retrieveOperations {
				operation.Trigger = &triggers[len(triggers)-1]
				changes = append(changes, operation)
			}
		}
	}
	return triggers, changes
}

// deploymentConfigTriggerIndexer converts deployment config events into entries for the trigger cache, and
// also calculates the latest state of the changes on the object.
type deploymentConfigTriggerIndexer struct {
	prefix string
}

func (i deploymentConfigTriggerIndexer) Index(obj, old interface{}) (string, *TriggerCacheEntry, []ImageChange, cache.DeltaType, error) {
	var (
		triggers []trigger.ObjectFieldTrigger
		changes  []ImageChange
		dc       *deployapi.DeploymentConfig
		change   cache.DeltaType
	)
	switch {
	case obj != nil && old == nil:
		// added
		dc = obj.(*deployapi.DeploymentConfig)
		triggers, changes = calculateDeploymentConfigTriggers(dc, true)
		change = cache.Added
	case old != nil && obj == nil:
		// deleted
		dc = old.(*deployapi.DeploymentConfig)
		triggers, _ = calculateDeploymentConfigTriggers(dc, false)
		change = cache.Deleted
	default:
		// updated
		dc = obj.(*deployapi.DeploymentConfig)
		triggers, changes = calculateDeploymentConfigTriggers(dc, true)
		oldTriggers, _ := calculateDeploymentConfigTriggers(old.(*deployapi.DeploymentConfig), false)
		switch {
		case len(oldTriggers) == 0:
			change = cache.Added
		case !reflect.DeepEqual(oldTriggers, triggers):
			change = cache.Updated
		}
	}

	if len(triggers) > 0 {
		key := i.prefix + dc.Namespace + "/" + dc.Name
		return key, &TriggerCacheEntry{
			Key:       key,
			Namespace: dc.Namespace,
			Triggers:  triggers,
		}, changes, change, nil
	}
	return "", nil, nil, change, nil
}

// DeploymentConfigReaction converts image trigger changes into updates on deployments.
type DeploymentConfigReaction struct {
	Client client.DeploymentConfigsNamespacer
}

// ImageChanged is passed a deployment config and a set of changes.
func (r *DeploymentConfigReaction) ImageChanged(obj interface{}, changes []ImageChange) error {
	dc := obj.(*deployapi.DeploymentConfig)
	copied, err := kapi.Scheme.DeepCopy(dc)
	if err != nil {
		return err
	}
	newDC := copied.(*deployapi.DeploymentConfig)

	count := 0
	for _, change := range changes {
		if len(change.Ref) == 0 {
			continue
		}
		if !strings.HasPrefix(change.Trigger.FieldPath, "spec.template.spec.") {
			continue
		}
		container, subselector := findPodSpecContainer(&newDC.Spec.Template.Spec, strings.TrimPrefix(change.Trigger.FieldPath, "spec.template.spec."))
		if container == nil || subselector != "image" {
			continue
		}
		if container.Image != change.Ref {
			container.Image = change.Ref
			count++
		}
		for _, trigger := range newDC.Spec.Triggers {
			p := trigger.ImageChangeParams
			if p == nil || !namesInclude(p.ContainerNames, container.Name) {
				continue
			}
			if p.LastTriggeredImage != change.Ref {
				p.LastTriggeredImage = change.Ref
				count++
			}
		}
	}

	if count > 0 {
		_, err := r.Client.DeploymentConfigs(dc.Namespace).Update(newDC)
		return err
	}

	return nil
}
