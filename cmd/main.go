package main

import (
	"context"
	"errors"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog"

	"github.com/vhlebnikov/raft/internal/handler"
	"github.com/vhlebnikov/raft/internal/node"
	"github.com/vhlebnikov/raft/internal/server"
)

// --node айди ноды
// --http - порт сервера, через который можно слать команды
// --cluster описание кластера, включая эту ноду, где nodeID:port,...

type config struct {
	nodeID     uint64
	httpPort   string
	clusterCfg []node.ClusterMember
}

func getConfig(logger zerolog.Logger) config {
	nodeID := flag.Uint64("node", 0, "this node ID")
	httpPort := flag.String("http", "", "port of the http user requests server")
	cluster := flag.String("cluster", "", "cluster config: nodeID,nodeRPCPort;etc...")

	flag.Parse()

	if nodeID == nil || (nodeID != nil && *nodeID == 0) {
		logger.Fatal().Msg("got empty or zero node id")
	}

	if httpPort == nil || (httpPort != nil && *httpPort == "") {
		logger.Fatal().Msg("got empty http port")
	}

	if cluster == nil || (cluster != nil && *cluster == "") {
		logger.Fatal().Msg("got empty cluster config")
	}

	var clusterCfg []node.ClusterMember

	for _, nodeFullCfg := range strings.Split(*cluster, ";") {
		if nodeFullCfg == "" {
			logger.Fatal().Msg("got empty cluster's node config")
		}

		nodeCfg := strings.Split(nodeFullCfg, ",")
		nodeCfgID, err := strconv.ParseUint(nodeCfg[0], 10, 64)
		if err != nil {
			logger.Fatal().Msg("can't parse id in cluster's node config")
		}
		clusterCfg = append(clusterCfg, node.ClusterMember{
			ID:      nodeCfgID,
			Address: nodeCfg[1],
		})
	}

	return config{
		nodeID:     *nodeID,
		httpPort:   *httpPort,
		clusterCfg: clusterCfg,
	}
}

const (
	_heartBeatInterval = 2 * time.Second
	_metadataDir       = "./meta"
)

func main() {
	var wg sync.WaitGroup

	logger := zerolog.New(
		zerolog.ConsoleWriter{
			Out:        os.Stderr,
			TimeFormat: time.RFC3339,
		},
	).Level(zerolog.TraceLevel).With().Timestamp().Caller().Logger()

	cfg := getConfig(logger)

	logger.Info().Msgf("config: %+v", cfg)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	raftNode := node.NewNode(cfg.nodeID, cfg.clusterCfg, _heartBeatInterval, _metadataDir, logger)
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := raftNode.Start(ctx); err != nil {
			logger.Fatal().Msgf("can't start raft node: %s", err)
		}
	}()
	logger.Info().Msgf("started new node with id: %d", cfg.nodeID)

	h := handler.NewRaftHandler(logger, raftNode)
	httpServer := server.NewHTTPServer(cfg.httpPort, h.InitRoutes())
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := httpServer.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatal().Msgf("can't start http server: %s", err)
		}
	}()
	logger.Info().Msgf("started http server on port: %s", cfg.httpPort)

	<-ctx.Done()
	logger.Info().Msg("got shutdown signal")

	if err := httpServer.Shutdown(ctx); err != nil {
		logger.Err(err).Msg("can't shutdown server")
	}

	logger.Info().Msg("shutdown http server")

	wg.Wait()
}
