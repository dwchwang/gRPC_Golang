package main

import (
	"context"
	"grpc-demo/pb"
	"io"
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

	// ── 6	. Gọi UploadFile (Client Streaming RPC) ───────────────────

	uploadStream, err := client.UploadFile(ctx)
	if err != nil {
		log.Fatalf("Khong the mo stream upload: %v", err)
	}

	fileData := []byte("Đây là nội dung của file cần upload qua stream... (gia lap 3 chunk)")
	chunkSize := 20
	seq := int32(1)

	for i := 0; i < len(fileData); i += chunkSize {
		end := i + chunkSize
		if end > len(fileData) {
			end = len(fileData)
		}

		err := uploadStream.Send(&pb.FileChunk{
			Filename: "test.txt",
			Data:     fileData[i:end],
			Seq:      seq,
		})
		if err != nil {
			log.Fatalf("Loi gui chunk: %v", err)
		}
		seq++
		time.Sleep(300 * time.Millisecond)
	}
	uploadResp, err := uploadStream.CloseAndRecv()
	if err != nil {
		log.Fatalf("Lỗi CloseAndRecv: %v", err)
	}
	log.Printf("Upload thành công! FileID=%s, TotalBytes=%d",
		uploadResp.FileId, uploadResp.TotalBytes)

	log.Println("=== 4. Bidi Streaming: Chat ===")
	startChat(client)
}

// function startChat
func startChat(client pb.UserServiceClient) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Mở bidi stream
	stream, err := client.Chat(ctx)
	if err != nil {
		log.Fatalf("Không thể mở chat stream: %v", err)
	}

	// ── Goroutine 1: nhận message từ server ──────────
	waitReceive := make(chan struct{})
	go func() {
		defer close(waitReceive)
		for {
			msg, err := stream.Recv()
			if err == io.EOF {
				log.Println("Server đóng stream")
				return
			}
			if err != nil {
				log.Printf("Lỗi nhận: %v", err)
				return
			}
			log.Printf("[%s]: %s", msg.User, msg.Text)
		}
	}()

	// ── Goroutine 2 (main): gửi message lên server ───
	messages := []string{
		"Xin chào server!",
		"gRPC Bidi Streaming hoạt động rồi!",
		"Tuyệt vời!",
	}

	for _, text := range messages {
		msg := &pb.ChatMessage{
			User:   "client",
			Text:   text,
			SentAt: time.Now().Unix(),
		}

		if err := stream.Send(msg); err != nil {
			log.Fatalf("Lỗi gửi: %v", err)
		}

		log.Printf("Đã gửi: %s", text)
		time.Sleep(1 * time.Second)
	}

	// Báo server client gửi xong
	stream.CloseSend()

	// Chờ goroutine nhận xử lý xong
	<-waitReceive
	log.Println("Chat kết thúc")
}
