package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFamilyOf(t *testing.T) {
	tests := map[string]string{
		"catalogue_family__en": "catalogue_family",
		"product__de_DE":       "product",
		"plain_route":          "other",
		"__leading":            "other",
		"":                     "other",
		"a__b__c":              "a",
	}
	for routeID, want := range tests {
		assert.Equal(t, want, familyOf(routeID), routeID)
	}
}
