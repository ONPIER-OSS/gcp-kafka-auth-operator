package helpers

import (
	"crypto/sha256"
	"fmt"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"
)

func SliceAppendIfMissing(slice []string, s string) []string {
	if len(slice) == 0 {
		return append(slice, s)
	}

	if slices.Contains(slice, s) {
		return slice
	}
	return append(slice, s)
}

func SliceRemoveItem(slice []string, s string) []string {
	newslice := []string{}

	for _, ele := range slice {
		if ele != s {
			newslice = append(newslice, ele)
		}
	}
	return newslice
}

func StringToSlice(input string) []string {
	return strings.Split(input, "\n")
}

func GetHashFromAnything(input any) (string, error) {
	inputYaml, err := yaml.Marshal(input)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(inputYaml)), nil
}
