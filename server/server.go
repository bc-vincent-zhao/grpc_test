package main

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	pb "github.com/bc-vincent-zhao/grpc_test/object"

	"golang.org/x/net/context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/reflection"
)

var (
	tls       = flag.Bool("tls", false, "Connection uses TLS if true, else plain TCP")
	certFile  = flag.String("cert_file", "testdata/server1.pem", "The TLS cert file")
	keyFile   = flag.String("key_file", "testdata/server1.key", "The TLS key file")
	dataFile  = flag.String("data_file", "testdata/content_data", "Some binary content to stream")
	http_port = flag.Int("http_port", 10001, "the port http endpoint listens on")
	grpc_port = flag.Int("grpc_port", 10002, "the port grpc endpoint listens on")
	verbose   = flag.Bool("verbose", false, "Log as much as debug info as possible, this slows down response time")

	content = []byte{}
	hash    = ""
	mtime   = ""
	err     error
)

func main() {
	flag.Parse()

	content, hash, mtime, err = readData()
	if err != nil {
		log.Fatalf("failed to start client %v", err)
	}

	go func() {
		// setup http server
		http.HandleFunc("/http", handleHttp)
		log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *http_port), nil))
	}()

	// setup grpc server
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *grpc_port))
	if err != nil {
		grpclog.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	if *tls {
		creds, err := credentials.NewServerTLSFromFile(*certFile, *keyFile)
		if err != nil {
			grpclog.Fatalf("Failed to generate credentials %v", err)
		}
		opts = []grpc.ServerOption{grpc.Creds(creds)}
	}
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterObjectAccessorServer(grpcServer, newServer())
	reflection.Register(grpcServer)
	grpcServer.Serve(lis)

}

func handleHttp(w http.ResponseWriter, r *http.Request) {
	if *verbose {
		addr := r.RemoteAddr
		log.Printf("remote addr: %v", addr)
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Last-Modified", mtime)
	w.Header().Set("Etag", hash)
	io.Copy(w, bytes.NewReader(content))
}

type server struct {
}

func newServer() *server {
	return &server{}
}

func (s *server) GetObject(ctx context.Context, id *pb.ObjectIdentifier) (*pb.ObjectResponse, error) {
	if *verbose {
		pr, ok := peer.FromContext(ctx)
		if ok {
			log.Printf("Unary: remote addr: %v", pr.Addr)
		}
	}
	return &pb.ObjectResponse{
		Type:    "application/octet-stream",
		Mtime:   mtime,
		Etag:    hash,
		Content: content,
	}, nil
}

func (s *server) GetObjectStream(stream pb.ObjectAccessor_GetObjectStreamServer) error {
	if *verbose {
		pr, ok := peer.FromContext(stream.Context())
		if ok {
			log.Printf("streaming: remote addr: %v", pr.Addr)
		}
	}
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		r := &pb.ObjectResponse{
			Type:    "application/octet-stream",
			Mtime:   mtime,
			Etag:    hash,
			Content: content,
		}
		stream.Send(r)
	}
}

func readData() (content []byte, hash, mtime string, err error) {
	fs, err := os.Open(*dataFile)
	if err != nil {
		return
	}
	defer fs.Close()

	stat, err := fs.Stat()
	if err != nil {
		return
	}

	md5 := md5.New()
	reader := io.TeeReader(fs, md5)

	content, err = ioutil.ReadAll(reader)
	hash = hex.EncodeToString(md5.Sum(nil))
	mtime = stat.ModTime().Format(time.RFC1123)
	return
}
