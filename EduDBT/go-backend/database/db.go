package database

import (
	"fmt"
	"log"

	"github.com/sleepbot-zapp/go-backend/config"
	"github.com/sleepbot-zapp/go-backend/models"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var DB *gorm.DB

func ConnectDB() {
	dsn := fmt.Sprintf(
		"host=%s user=%s password=%s dbname=%s port=%s sslmode=disable",
		config.GetEnv("DB_HOST"),
		config.GetEnv("DB_USER"),
		config.GetEnv("DB_PASS"),
		config.GetEnv("DB_NAME"),
		config.GetEnv("DB_PORT"),
	)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("❌ Failed to connect to database: %v", err)
	}

	// Auto migrate models
	err = db.AutoMigrate(&models.User{})
	if err != nil {
		log.Fatalf("❌ Migration failed: %v", err)
	}

	DB = db
	log.Println("✅ Connected to Postgres & migrated schema")
}
