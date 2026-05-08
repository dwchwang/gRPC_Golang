package main

import (
	"context"
	"grpc-demo/pb"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// ── 1. Tạo connection đến server ─────────────────

	// insecure.NewCredentials() = không dùng TLS (chỉ dùng khi dev)
	conn, err := grpc.NewClient(
		"localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	if err != nil {
		log.Fatal("Khong the ket noi: %w", err)
	}
	defer conn.Close()

	// ── 2. Tạo client stub (auto-generated) ──────────
	client := pb.NewUserServiceClient(conn)

	// ── 3. Gọi CreateUser (Unary RPC) ────────────────

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	createResp, err := client.CreateUser(ctx, &pb.CreateUserRequest{
		Name:  "Duc hoang",
		Email: "baohoangbh@gmail.com",
		Age:   21,
	})
	if err != nil {
		log.Fatal("create user that bai: %w", err)
	}

	log.Printf("Đã tạo user: ID=%s, Name=%s", createResp.Id, createResp.Name)

	// ── 4. Gọi GetUser (Unary RPC) ───────────────────
	getResp, err := client.GetUser(ctx, &pb.GetUserRequest{
		Id: createResp.Id,
	})
	if err != nil {
		log.Fatalf("GetUser thất bại: %v", err)
	}
	log.Printf("Lấy user thành công: %s - %s", getResp.Name, getResp.Email)

	// ── 5. Gọi Get List User (Server Streaming RPC) ───────────────────

	stream, err := client.ListUsers(ctx, &pb.UserListRequest{Limit: 10})
	if err != nil {
		log.Fatal("ListUsers That bai : %w", err)
	}

	// Lap qua stream
	for {
		user, err := stream.Recv()
		if err != nil {
			// io.EOF = server đã đóng stream bình thường
			if err.Error() == "EOF" {
				log.Println("Stream kết thúc")
				break
			}
			log.Fatalf("Lỗi nhận stream: %v", err)
		}
		log.Printf("Nhan duoc user: %s - %s", user.Name, user.Email)
	}
}
