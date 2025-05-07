package helpers

func SliceAppendIfMissing(slice []string, s string) []string {
	if len(slice) == 0 {
		return append(slice, s)
	}

	for _, ele := range slice {
		if ele == s {
			return slice
		}
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
