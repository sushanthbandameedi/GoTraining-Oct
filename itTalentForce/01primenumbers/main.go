//Assignment 1: create a program to get the prime numbers between 1 to 10 and sum those prime numbers

//Assignment 2: create a calculator program where add, multiplication, sub and divide function should be in custom package and call those functions through func main

package main

import "fmt"

func isPrime(num int) bool {
	if num <= 1 {
		return false
	}

	for i := 2; i*i <= num; i++ { //check divisiblity till the squareroot of num as the factors repeat later
		if num%i == 0 {
			return false
		}
	}
	return true
}

func main() {

	primeNumbers := []int{}
	sum := 0

	for num := 1; num <= 10; num++ {
		if isPrime(num) {
			primeNumbers = append(primeNumbers, num)
			sum = sum + num

		}
	}

	fmt.Println("Prime numbers from 1 to 10:", primeNumbers)
	fmt.Println("Sum of Prime numbers is:", sum)

}
