package agg

import (
	"github.com/oriser/regroup"
)

// ReGroup wraps regroup.ReGroup to add YAML marshalling from and to a (regex) string
type ReGroup struct {
	*regroup.ReGroup
	original string
}

func NewReGroup(original string) ReGroup {
	return ReGroup{
		ReGroup:  regroup.MustCompile(original),
		original: original,
	}
}

func (r *ReGroup) UnmarshalYAML(unmarshal func(any) error) error {
	if err := unmarshal(&r.original); err != nil {
		return err
	}
	reGroup, err := regroup.Compile(r.original)
	if err != nil {
		return err
	}
	r.ReGroup = reGroup
	return nil
}

func (r ReGroup) MarshalYAML() (interface{}, error) {
	if r.ReGroup == nil {
		return "", nil
	}
	return r.original, nil
}
