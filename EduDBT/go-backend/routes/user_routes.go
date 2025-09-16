package routes

import (
	"github.com/gin-gonic/gin"
	"github.com/sleepbot-zapp/go-backend/controllers"
)

func UserRoutes(r *gin.Engine) {
	r.POST("/save-user", controllers.SaveUser)
}
