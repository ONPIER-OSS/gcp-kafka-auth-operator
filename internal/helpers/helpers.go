package helpers

import (
	"crypto/sha256"
	"fmt"
	"regexp"
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

func StringSanitize(s string, limit int) string {
	s = strings.ToLower(s)
	unsupportedChars := regexp.MustCompile(`[^0-9a-zA-Z$-]`)
	s = unsupportedChars.ReplaceAllString(s, "-")

	if len(s) <= limit {
		return s
	}

	hash := fmt.Sprintf("%x", sha256.Sum256([]byte(s)))

	if limit <= 9 {
		return hash[:limit]
	}

	return fmt.Sprintf("%s-%s", s[:limit-9], hash[:8])
}
