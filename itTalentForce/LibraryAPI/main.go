package main

import (
	"net/http"

	"errors"

	"github.com/gin-gonic/gin"
)

type book struct {
	ID       string `json:"id"`
	Title    string `json:"title"`
	Author   string `json:"author"`
	Quantity int    `json:"quantity"`
}

var books = []book{
	{ID: "1", Title: "3 mistakes of my life", Author: "Chetan Bhagat", Quantity: 5},
	{ID: "2", Title: "1984", Author: "George Orwell", Quantity: 7},
	{ID: "3", Title: "Harry Potter", Author: "JK Rowling", Quantity: 9},
}

func getBooks(c *gin.Context) {
	c.IndentedJSON(http.StatusOK, books)
}

func bookById(c *gin.Context) {
	id := c.Param("id")
	book, err := getBookById(id)
	if err != nil {
		c.IndentedJSON(http.StatusNotFound, gin.H{"message": "Book not found."})
	}
	c.IndentedJSON(http.StatusOK, book)
}

func returnBook(c *gin.Context) {
	id, ok := c.GetQuery("id")

	if !ok {
		c.IndentedJSON(http.StatusBadRequest, gin.H{"message": "missing id query parameter."})
		return
	}

	book, err := getBookById(id)
	if err != nil {
		c.IndentedJSON(http.StatusNotFound, gin.H{"message": "book not found"})
		return
	}

	book.Quantity += 1
	c.IndentedJSON(http.StatusOK, book)
}

func getBookById(id string) (*book, error) {
	for i, b := range books {
		if b.ID == id {
			return &books[i], nil
		}
	}
	return nil, errors.New("book not found")
}

func checkoutBook(c *gin.Context) {
	id, ok := c.GetQuery("id")

	if !ok {
		c.IndentedJSON(http.StatusBadRequest, gin.H{"message": "missing id query parameter."})
		return
	}

	book, err := getBookById(id)
	if err != nil {
		c.IndentedJSON(http.StatusNotFound, gin.H{"message": "book not found"})
		return
	}

	if book.Quantity <= 0 {
		c.IndentedJSON(http.StatusBadRequest, gin.H{"message": "book not available in the inventory"})
		return
	}

	book.Quantity -= 1
	c.IndentedJSON(http.StatusOK, book)

}

func createBook(c *gin.Context) {
	var newBook book

	if err := c.BindJSON(&newBook); err != nil {
		return
	}
	books = append(books, newBook)
	c.IndentedJSON(http.StatusCreated, newBook)

}

func main() {
	router := gin.Default()
	router.GET("/books", getBooks)          // curl localhost:8080/books
	router.GET("/books/:id", bookById)      // curl localhost:8080/books/1
	router.POST("/books", createBook)       // curl localhost:8080/books --include --header "Content-Type: application/json" -d @body.json --request "POST"
	router.PATCH("/checkout", checkoutBook) // curl localhost:8080/checkout\?id=1 --request "PATCH"
	router.PATCH("/return", returnBook)     // curl localhost:8080/return\?id=1 --request "PATCH"
	router.Run("localhost:8080")
}
