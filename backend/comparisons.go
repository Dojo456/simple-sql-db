package backend

func compareValues(v1 []byte, operator Operator, v2 []byte, as Primitive) bool {
	if len(v1) != len(v2) {
		panic("slice lengths do not match. cannot compare.")
	}

	switch operator {
	case OperatorEqual:
		return compareEqual(v1, v2, as)
	case OperatorNotEqual:
		return !compareEqual(v1, v2, as)
	case OperatorLessThan:
		return compareLessThan(v1, v2, as)
	case OperatorLessThanOrEqual:
		return compareLessThanOrEqual(v1, v2, as)
	case OperatorGreaterThan:
		return compareGreaterThan(v1, v2, as)
	case OperatorGreaterThanOrEqual:
		return compareGreaterThanOrEqual(v1, v2, as)
	}

	return false
}

func compareEqual(v1 []byte, v2 []byte, as Primitive) bool {
	for i := range v1 {
		if v1[i] != v2[i] {
			return false
		}
	}

	return true
}

func compareLessThan(v1 []byte, v2 []byte, as Primitive) bool {
	for i := range v1 {
		if v1[i] < v2[i] {
			return true
		}
	}

	return false
}

func compareLessThanOrEqual(v1 []byte, v2 []byte, as Primitive) bool {
	allEqual := true

	for i := range v1 {
		if v1[i] < v2[i] {
			return true
		}

		allEqual = allEqual && v1[i] == v2[i]
	}

	return allEqual
}

func compareGreaterThan(v1 []byte, v2 []byte, as Primitive) bool {
	for i := range v1 {
		if v1[i] > v2[i] {
			return true
		}
	}

	return false
}

func compareGreaterThanOrEqual(v1 []byte, v2 []byte, as Primitive) bool {
	allEqual := true

	for i := range v1 {
		if v1[i] > v2[i] {
			return true
		}

		allEqual = allEqual && v1[i] == v2[i]
	}

	return allEqual
}
