package database

import (
	"log"

	"github.com/sleepbot-zapp/go-backend/models"
)

func Migrate() {
	err := DB.AutoMigrate(&models.User{}) // add more models here
	if err != nil {
		log.Fatal("❌ Migration failed:", err)
	}
	log.Println("✅ Database migrated!")
}
