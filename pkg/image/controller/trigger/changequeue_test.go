package trigger

import (
	"testing"

	"k8s.io/kubernetes/pkg/util/diff"

	"github.com/openshift/origin/pkg/image/api/v1/trigger"
)

func TestMergeOperations(t *testing.T) {
	trigger1 := &trigger.ObjectFieldTrigger{
		From: trigger.ObjectReference{
			Kind: "ImageStreamTag",
			Name: "stream:1",
		},
		FieldPath: "test.path.1",
	}

	testCases := []struct {
		original []ImageChange
		merge    []ImageChange
		expected []ImageChange
	}{
		{
			merge:    []ImageChange{{"stream:1", "1", "image:1", "test", 1, trigger1}},
			expected: []ImageChange{{"stream:1", "1", "image:1", "test", 1, trigger1}},
		},
		{
			original: []ImageChange{{"stream:1", "1", "image:0", "test", 1, trigger1}},
			merge:    []ImageChange{{"stream:1", "1", "image:1", "test", 2, trigger1}},
			expected: []ImageChange{{"stream:1", "1", "image:1", "test", 2, trigger1}},
		},
		{
			original: []ImageChange{{"stream:1", "1", "image:0", "test", 1, trigger1}},
			merge:    []ImageChange{{"stream:1", "1", "image:1", "test", 3, trigger1}, {"stream:1", "1", "image:2", "test", 2, trigger1}},
			expected: []ImageChange{{"stream:1", "1", "image:1", "test", 3, trigger1}},
		},
		{
			original: []ImageChange{{"stream:2", "1", "image:0", "test", 1, trigger1}},
			merge:    []ImageChange{{"stream:1", "1", "image:1", "test", 0, trigger1}},
			expected: []ImageChange{{"stream:1", "1", "image:1", "test", 0, trigger1}},
		},
	}
	for i, test := range testCases {
		result := mergeOperations(test.original, test.merge)
		if !operationsEqual(test.expected, result) {
			t.Errorf("%d: %s", i, diff.ObjectReflectDiff(test.expected, result))
		}
	}
}
