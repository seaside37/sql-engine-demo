package main

import (
	"log"

	"sql-engine/database"
	"sql-engine/handlers"

	"github.com/gin-gonic/gin"
)

func main() {
	// Initialize database
	dsn := "postgres://postgres:123456@localhost:5432/tsdb"
	if err := database.Init(dsn); err != nil {
		log.Fatal("Database connection failed:", err)
	}
	defer database.Close()

	// Create handlers
	handler := handlers.NewHandler(database.DB)

	// Setup routes
	r := gin.Default()

	r.Use(func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	})

	// Schema routes
	r.GET("/databases", handler.GetDatabases)
	r.GET("/tables", handler.GetTables)
	r.GET("/table/:name/columns", handler.GetTableColumns)
	r.GET("/table/:name/primary-keys", handler.GetTablePrimaryKeys)
	r.GET("/table/:name/foreign-keys", handler.GetTableForeignKeys)
	r.GET("/schema", handler.GetFullSchema)

	// Query route
	r.POST("/run-query", handler.RunQuery)

	// Start server
	log.Println("Server starting on :8080")
	if err := r.Run(":8080"); err != nil {
		log.Fatal("Server failed to start:", err)
	}
}
