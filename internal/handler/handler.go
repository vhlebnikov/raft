package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog"

	"github.com/vhlebnikov/raft/internal/node"
	"github.com/vhlebnikov/raft/internal/statemachine"
)

type RaftHandler struct {
	logger   zerolog.Logger
	raftNode *node.Node
}

func NewRaftHandler(logger zerolog.Logger, raftNode *node.Node) *RaftHandler {
	return &RaftHandler{
		logger:   logger,
		raftNode: raftNode,
	}
}

func (h *RaftHandler) InitRoutes() http.Handler {
	r := gin.New()

	r.GET("/get", h.GetHandler)
	r.POST("/set", h.SetHandler)

	return r
}

// GetHandler
// /get?key=x
func (h *RaftHandler) GetHandler(ctx *gin.Context) {
	cmd := statemachine.Command{
		Type: statemachine.GetCommand,
		Key:  ctx.Query("key"),
	}

	if cmd.Key == "" {
		ctx.String(http.StatusBadRequest, "empty key query parameter")
		return
	}

	results, err := h.raftNode.Apply([]statemachine.Command{cmd})
	if err != nil {
		ctx.String(http.StatusBadRequest, "can't get value: %s", err)
		return
	}

	if len(results) != 1 {
		ctx.String(http.StatusInternalServerError, "expected single response value, but got: %d", len(results))
		return
	}

	ctx.JSON(http.StatusOK, results[0])
}

// SetHandler
// /set?key=x&value=y
func (h *RaftHandler) SetHandler(ctx *gin.Context) {
	cmd := statemachine.Command{
		Type:  statemachine.SetCommand,
		Key:   ctx.Query("key"),
		Value: ctx.Query("value"),
	}

	// empty value param is ok
	if cmd.Key == "" {
		ctx.String(http.StatusBadRequest, "empty key query parameter")
		return
	}

	if _, err := h.raftNode.Apply([]statemachine.Command{cmd}); err != nil {
		ctx.String(http.StatusBadRequest, "can't set value: %s", err)
		return
	}

	ctx.String(http.StatusOK, "ok")
}
