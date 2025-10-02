package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"instorage-manager/pkg/manager"
	pb "instorage-manager/pkg/proto"
)

const (
	DefaultGRPCPort = "50051"
	DefaultHTTPPort = "8080"
)

var (
	setupLog = ctrl.Log.WithName("setup")
)

func main() {
	var (
		grpcPort = flag.String("grpc-port", DefaultGRPCPort, "gRPC server port")
		httpPort = flag.String("http-port", DefaultHTTPPort, "HTTP server port for webhooks")
		nodeName = flag.String("node-name", "", "Node name this manager runs on")
	)

	// Setup logging
	opts := zap.Options{
		Development: true,
		TimeEncoder: func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
			enc.AppendString(t.Format("2006-01-02 15:04:05"))
		},
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// Get node name from environment if not provided
	if *nodeName == "" {
		*nodeName = os.Getenv("NODE_NAME")
		if *nodeName == "" {
			setupLog.Error(fmt.Errorf("node name not provided"),
				"node name must be set via --node-name flag or NODE_NAME env var")
			os.Exit(1)
		}
	}

	setupLog.Info("Starting Instorage Manager", "nodeName", *nodeName, "grpcPort", *grpcPort, "httpPort", *httpPort)

	// Create gRPC server
	grpcServer := grpc.NewServer()

	// Create and register our service
	instorageServer := manager.NewInstorageManagerServer(ctrl.Log.WithName("manager"), *nodeName)
	pb.RegisterInstorageManagerServer(grpcServer, instorageServer)

	// Register health service
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	// Create HTTP server for webhooks
	gin.SetMode(gin.ReleaseMode)
	httpRouter := gin.New()
	httpRouter.Use(gin.Logger(), gin.Recovery())
	
	// Setup webhook endpoints
	instorageServer.SetupWebhookRoutes(httpRouter)
	
	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%s", *httpPort),
		Handler: httpRouter,
	}

	// Listen on the specified port
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", *grpcPort))
	if err != nil {
		setupLog.Error(err, "Failed to listen on port", "port", *grpcPort)
		os.Exit(1)
	}

	// Start gRPC server in a goroutine
	go func() {
		setupLog.Info("Starting gRPC server", "address", lis.Addr().String())
		if err := grpcServer.Serve(lis); err != nil {
			setupLog.Error(err, "gRPC server failed")
			os.Exit(1)
		}
	}()

	// Start HTTP server in a goroutine
	go func() {
		setupLog.Info("Starting HTTP webhook server", "address", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			setupLog.Error(err, "HTTP server failed")
			os.Exit(1)
		}
	}()

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for shutdown signal
	sig := <-sigChan
	setupLog.Info("Received shutdown signal", "signal", sig.String())

	// Graceful shutdown
	setupLog.Info("Shutting down servers...")
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	// Create context with timeout for graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Stop HTTP server gracefully
	if err := httpServer.Shutdown(ctx); err != nil {
		setupLog.Error(err, "HTTP server forced to shutdown")
	}

	// Stop gRPC server gracefully
	grpcServer.GracefulStop()

	setupLog.Info("Instorage Manager stopped")
}

// // loggingInterceptor logs all gRPC calls
// func loggingInterceptor(logger logr.Logger) grpc.UnaryServerInterceptor {
// 	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
// 		start := time.Now()

// 		// Call the handler
// 		resp, err := handler(ctx, req)

// 		duration := time.Since(start)

// 		if err != nil {
// 			logger.Error(err, "gRPC call failed",
// 				"method", info.FullMethod,
// 				"duration", duration,
// 			)
// 		} else {
// 			logger.V(1).Info("gRPC call completed",
// 				"method", info.FullMethod,
// 				"duration", duration,
// 			)
// 		}

// 		return resp, err
// 	}
// }
