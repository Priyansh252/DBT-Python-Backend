package main

import (
	"github.com/gin-gonic/gin"
	"github.com/sleepbot-zapp/go-backend/config"
	"github.com/sleepbot-zapp/go-backend/database"
	"github.com/sleepbot-zapp/go-backend/models"
	"github.com/sleepbot-zapp/go-backend/routes"
)

func main() {
	config.LoadEnv()
	database.ConnectDB()
	database.DB.AutoMigrate(&models.User{})

	r := gin.Default()
	routes.UserRoutes(r)

	r.Run(":3200")
}
