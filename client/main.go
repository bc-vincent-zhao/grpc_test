package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"golang.org/x/net/context"

	pb "github.com/bc-vincent-zhao/grpc_tryout/object"
	"google.golang.org/grpc"
)

var (
	loop      = flag.Int("loop", 10, "loop count for reading object")
	protocol  = flag.String("protocol", "all", "the protocol to use, http|grpc|all")
	http_port = flag.Int("http_port", 10001, "the port http endpoint listens on")
	grpc_port = flag.Int("grpc_port", 10002, "the port grpc endpoint listens on")
)

func main() {
	flag.Parse()

	if *protocol == "grpc" {
		getGrpc(*loop)
	} else if *protocol == "http" {
		getHttp(*loop)
	} else {
		getHttp(*loop)
		getGrpc(*loop)
		streamGrpc(*loop)
	}
}

func getHttp(loop int) {
	start := time.Now()
	client := &http.Client{}
	var bytes []byte
	//wg := &sync.WaitGroup{}
	for i := 0; i < loop; i++ {
		//wg.Add(1)
		//go func() {
		//defer wg.Done()
		req, _ := http.NewRequest("GET", fmt.Sprintf("http://localhost:%d/http", *http_port), nil)
		resp, err := client.Do(req)
		if err != nil {
			log.Printf("could not get object: %v", err)
		} else {
			bytes, _ = ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			//log.Printf("got object: %v", string(bytes))
		}
		//}()
	}
	//wg.Wait()
	duration := time.Since(start)
	log.Printf("Through http: size = %v ;duration = %v", len(bytes), duration)
}

func getGrpc(loop int) {
	start := time.Now()
	conn, err := grpc.Dial(fmt.Sprintf("localhost:%d", *grpc_port), grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewObjectAccessorClient(conn)

	var r *pb.ObjectResponse
	//wg := &sync.WaitGroup{}
	for i := 0; i < loop; i++ {
		//wg.Add(1)
		//go func() {
		//defer wg.Done()
		r, err = c.GetObject(context.Background(), &pb.ObjectIdentifier{"test", "manual", "dummy"})
		if err != nil {
			log.Printf("could not get object: %v", err)
		} else {
			_ = r.Content
			//log.Printf("got object: %v", r)
		}
		//}()
	}
	//wg.Wait()
	duration := time.Since(start)
	log.Printf("Through grpc, size = %v; duration = %v", len(r.Content), duration)
}

func streamGrpc(loop int) {
	start := time.Now()
	conn, err := grpc.Dial(fmt.Sprintf("localhost:%d", *grpc_port), grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewObjectAccessorClient(conn)
	stream, err := c.GetObjectStream(context.Background())
	if err != nil {
		log.Fatalf("can't get stream client %v", err)
	}

	waitc := make(chan struct{})
	go func() {
		var in *pb.ObjectResponse
		var err error
		for {
			in, err = stream.Recv()
			if err == io.EOF {
				close(waitc)
				return
			}
			if err != nil {
				log.Printf("Failed to receive object %v", err)
				return
			}
			_ = in.Content
		}
	}()
	for i := 0; i < loop; i++ {
		if err := stream.Send(&pb.ObjectIdentifier{"test", "manual", "dummy"}); err != nil {
			log.Fatalf("Failed to send id: %v", err)
		}
	}
	stream.CloseSend()
	<-waitc
	duration := time.Since(start)
	log.Printf("Through grpc stream, duration = %v", duration)
}
