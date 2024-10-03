package main

import (
	"fmt"
	"math/rand"
	"net/http"
	"time"
)

func main() {
	mux := http.NewServeMux()

	// Seed the random number generator
	rand.Seed(time.Now().UnixNano())

	mux.HandleFunc("/send-me-request", func(w http.ResponseWriter, r *http.Request) {
		// Simulate random delay
		delay := time.Duration(rand.Intn(500)) * time.Millisecond // Random delay between 0 to 1000 ms
		time.Sleep(delay)

		// Randomly decide whether to fail or succeed
		if rand.Intn(100) < 30 { // 30% chance of failure
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			fmt.Println("Failed: Internal Server Error")
			return
		}

		// Successful response
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "Request successful")
		fmt.Println("Success: Request handled successfully")
	})

	fmt.Println("Server started at :8999")
	http.ListenAndServe(":8999", mux)
}
