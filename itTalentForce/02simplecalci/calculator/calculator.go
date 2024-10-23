package calculator

import "fmt"

func Add(x int, y int) int {
	return x + y
}

func Multiply(x int, y int) int {
	return x * y
}

func Subtract(x int, y int) int {
	return x - y
}

func Divide(x int, y int) (int, error) {
	if y == 0 {
		return 0, fmt.Errorf("ERROR: Division by zero is not allowed")
	} else if x%y != 0 {
		return x / y, fmt.Errorf("division resulted in a remainder of %d", x%y)
	}

	return x / y, nil
}
