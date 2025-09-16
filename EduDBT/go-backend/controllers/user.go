package controllers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sleepbot-zapp/go-backend/database"
	"github.com/sleepbot-zapp/go-backend/models"
)

func SaveUser(c *gin.Context) {
	var user models.User

	if err := c.ShouldBindJSON(&user); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Upsert (insert or update if exists)
	err := database.DB.Where(models.User{Email: user.Email}).
		Assign(models.User{Name: user.Name, Image: user.Image}).
		FirstOrCreate(&user).Error

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save user"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true, "user": user})
}
