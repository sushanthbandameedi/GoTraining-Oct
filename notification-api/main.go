package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"

	"github.com/IBM/sarama"

	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
)

// Subscription structure to hold user subscription details
type Subscription struct {
	UserID               string   `json:"user_id"`
	Topics               []string `json:"topics"`
	NotificationChannels struct {
		Email             string `json:"email"`
		SMS               string `json:"sms"`
		PushNotifications bool   `json:"push_notifications"`
	} `json:"notification_channels"`
}

// Global variable for database connection
var db *sql.DB

func main() {
	// Initialize MySQL connection
	dsn := "root:password@tcp(127.0.0.1:3306)/notification_service"
	var err error
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal("Failed to connect to MySQL:", err)
	}

	// Ping the database to ensure it's connected
	err = db.Ping()
	if err != nil {
		log.Fatal("Failed to ping MySQL:", err)
	}
	log.Println("Connected to MySQL!")

	// Initialize Gin router
	router := gin.Default()

	// 1. POST /subscribe endpoint to handle subscription requests
	router.POST("/subscribe", func(c *gin.Context) {
		var newSubscription Subscription

		// Bind the incoming JSON data to the Subscription struct
		if err := c.ShouldBindJSON(&newSubscription); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		// Insert subscription for each topic into the MySQL database
		insertQuery := "INSERT INTO subscriptions (user_id, topic, email, sms, push_notifications) VALUES (?, ?, ?, ?, ?)"
		for _, topic := range newSubscription.Topics {
			_, err := db.Exec(insertQuery, newSubscription.UserID, topic, newSubscription.NotificationChannels.Email, newSubscription.NotificationChannels.SMS, newSubscription.NotificationChannels.PushNotifications)
			if err != nil {
				log.Println("Error inserting into MySQL:", err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save subscription"})
				return
			}
		}

		// Respond with success message
		c.JSON(http.StatusOK, gin.H{
			"message": "Subscription successful",
			"data":    newSubscription,
		})
	})

	// 2. Unsubscribe API: POST /unsubscribe
	router.POST("/unsubscribe", func(c *gin.Context) {
		// Define the structure of the request body
		var unsubscribeRequest struct {
			UserID string   `json:"user_id"`
			Topics []string `json:"topics"`
		}

		// Bind the incoming JSON request to unsubscribeRequest struct
		if err := c.ShouldBindJSON(&unsubscribeRequest); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		// Loop through the list of topics to unsubscribe the user from
		for _, topic := range unsubscribeRequest.Topics {
			// Delete the subscription from the database
			_, err := db.Exec("DELETE FROM subscriptions WHERE user_id = ? AND topic = ?", unsubscribeRequest.UserID, topic)
			if err != nil {
				log.Println("Error deleting subscription:", err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to unsubscribe from topic: " + topic})
				return
			}
		}

		// Respond with success message
		c.JSON(http.StatusOK, gin.H{
			"message":             "Unsubscription successful",
			"user_id":             unsubscribeRequest.UserID,
			"unsubscribed_topics": unsubscribeRequest.Topics,
		})
	})

	// 3. Fetch User Subscriptions API: GET /subscriptions/:user_id
	router.GET("/subscriptions/:user_id", func(c *gin.Context) {
		userID := c.Param("user_id") // Get user_id from the URL parameter

		// Query the database to get the user's active subscriptions
		rows, err := db.Query("SELECT topic, email, sms, push_notifications FROM subscriptions WHERE user_id = ?", userID)
		if err != nil {
			log.Println("Error fetching subscriptions:", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch subscriptions"})
			return
		}
		defer rows.Close()

		// Define a slice to store the subscriptions
		var subscriptions []map[string]interface{}

		// Iterate through the result set and build the response
		for rows.Next() {
			var topic, email, sms string
			var pushNotifications bool

			// Scan each row into the variables
			if err := rows.Scan(&topic, &email, &sms, &pushNotifications); err != nil {
				log.Println("Error scanning row:", err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch subscriptions"})
				return
			}

			// Build the subscription map and add it to the subscriptions slice
			subscription := map[string]interface{}{
				"topic": topic,
				"channels": map[string]interface{}{
					"email":              email,
					"sms":                sms,
					"push_notifications": pushNotifications,
				},
			}
			subscriptions = append(subscriptions, subscription)
		}

		// Check if no subscriptions were found
		if len(subscriptions) == 0 {
			c.JSON(http.StatusNotFound, gin.H{"message": "No subscriptions found for user", "user_id": userID})
			return
		}

		// Return the list of subscriptions
		c.JSON(http.StatusOK, gin.H{
			"user_id":       userID,
			"subscriptions": subscriptions,
		})
	})

	// 4. POST /notifications/send endpoint to send notifications
	router.POST("/notifications/send", func(c *gin.Context) {
		var notificationRequest struct {
			Topic string `json:"topic"`
			Event struct {
				EventID   string                 `json:"event_id"`
				Timestamp string                 `json:"timestamp"`
				Details   map[string]interface{} `json:"details"`
			} `json:"event"`
			Message struct {
				Title string `json:"title"`
				Body  string `json:"body"`
			} `json:"message"`
		}

		// Bind the incoming JSON request to notificationRequest struct
		if err := c.ShouldBindJSON(&notificationRequest); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		// Set the Kafka topic based on the notification type
		var kafkaTopic string
		switch notificationRequest.Topic {
		case "email":
			kafkaTopic = "email_topic"
		case "sms":
			kafkaTopic = "sms_topic"
		case "in_app":
			kafkaTopic = "in_app_topic"
		default:
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid topic"})
			return
		}

		// Format the message content for Kafka
		messageContent := fmt.Sprintf("Title: %s\nBody: %s", notificationRequest.Message.Title, notificationRequest.Message.Body)

		// Send the message to the specified Kafka topic
		err := produceMessage(kafkaTopic, messageContent)
		if err != nil {
			log.Println("Error producing Kafka message:", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to send notification"})
			return
		}

		// Return success response
		c.JSON(http.StatusOK, gin.H{
			"message":      "Notification sent successfully to Kafka",
			"topic":        notificationRequest.Topic,
			"notification": notificationRequest.Message,
		})
	})

	// Start Kafka consumers for each topic concurrently
	go consumeMessages("email_topic")
	go consumeMessages("sms_topic")
	go consumeMessages("in_app_topic")

	// Start the HTTP server in the main thread to handle API requests
	go func() {
		log.Println("Starting the API server on port 8080...")
		if err := router.Run(":8080"); err != nil {
			log.Fatal("Failed to start server:", err)
		}
	}()

	// Block main function to keep consumers running
	select {}

}

// produceMessage sends a message to a specified Kafka topic
func produceMessage(topic, message string) error {
	// Initialize a new Kafka producer
	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, nil)
	if err != nil {
		return fmt.Errorf("failed to create Kafka producer: %w", err)
	}
	defer producer.Close() // Ensure producer is closed after use

	// Create a Kafka message
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	// Send the message and log partition/offset information
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to send message to topic %s: %w", topic, err)
	}
	log.Printf("Message sent to topic %s on partition %d at offset %d\n", topic, partition, offset)
	return nil
}

// Function to start a Kafka consumer for a given topic
func consumeMessages(topic string) {
	config := sarama.NewConfig()
	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("Error creating Kafka consumer: %v", err)
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("Error creating partition consumer for topic %s: %v", topic, err)
	}
	defer partitionConsumer.Close()

	log.Printf("Started consumer for topic: %s", topic)

	// Process messages as they arrive
	for message := range partitionConsumer.Messages() {
		log.Printf("Received message on %s: %s\n", topic, string(message.Value))
		// Can add processing logic here (e.g., send email, SMS, or in-app notification) in future
	}
}
