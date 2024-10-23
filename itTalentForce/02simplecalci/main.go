package main

import (
	"bufio"
	"calulator/calculator"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func main() {

	// Reading input1 from user
	fmt.Println("Please give value for x:")
	reader := bufio.NewReader(os.Stdin)
	input1, _ := reader.ReadString('\n')

	x, err := strconv.Atoi(strings.TrimSpace(input1))
	if err != nil {
		fmt.Println("Invalid input. Please enter a valid number.")
		return
	}
	fmt.Println("Value of x is:", x)

	// reading input2 from user
	fmt.Println("Please give value for y:")
	input2, _ := reader.ReadString('\n')

	y, err := strconv.Atoi(strings.TrimSpace(input2))
	if err != nil {
		fmt.Println("Invalid input. Please enter a valid number.")
		return
	}
	fmt.Println("Value of y is:", y)

	// reading operation from user
	fmt.Print("Enter the operation (+, -, *, /): ")
	inputOperation, _ := reader.ReadString('\n')
	inputOperation = strings.TrimSpace(inputOperation)

	// performing the operation
	switch inputOperation {
	case "+":
		result := calculator.Add(x, y)
		fmt.Printf("Result of %d + %d is: %d\n", x, y, result)
	case "-":
		result := calculator.Subtract(x, y)
		fmt.Printf("Result of %d - %d is: %d\n", x, y, result)
	case "*":
		result := calculator.Multiply(x, y)
		fmt.Printf("Result of %d * %d is: %d\n", x, y, result)
	case "/":
		result, err := calculator.Divide(x, y)
		fmt.Printf("Result of %d / %d is: %d\n", x, y, result)
		if err != nil {
			fmt.Println("Error:", err)
		}

	default:
		fmt.Println("Invalid operation. Please enter one of +, -, *, or /.")
	}

}
