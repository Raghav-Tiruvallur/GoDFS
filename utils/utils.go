package utils

func ErrorHandler(err error) {
	if err != nil {
		panic(err)
	}
}

func ValueInArray(value string, array []string) bool {

	for _, val := range array {
		if val == value {
			return true
		}
	}
	return false

}
