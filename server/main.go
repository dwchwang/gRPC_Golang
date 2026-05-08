package main

import (
	"context"
	"fmt"
	"grpc-demo/pb"
	"io"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
)

// ── 1. Định nghĩa struct implement UserServiceServer ──
type userServer struct {
	// PHẢI embed cái này để thoả mãn interface
	pb.UnimplementedUserServiceServer

	// Giả lập database bằng map (thực tế dùng DB thật)
	users map[string]*pb.CreateUserResponse
}

// ── 2. Implement từng method trong interface ──────────

func (s *userServer) CreateUser(
	ctx context.Context,
	req *pb.CreateUserRequest,
) (*pb.CreateUserResponse, error) {

	// tao user moi
	id := fmt.Sprintf("user-%d", len(s.users)+1)
	user := &pb.CreateUserResponse{
		Id:    id,
		Name:  req.Name,
		Email: req.Email,
	}

	// luu vao DB
	s.users[id] = user

	log.Printf("Create User: %s (%s)", req.Name, req.Email)
	return user, nil
}

func (s *userServer) GetUser(
	ctx context.Context,
	req *pb.GetUserRequest,
) (*pb.CreateUserResponse, error) {
	user, exists := s.users[req.Id]
	if !exists {
		return nil, fmt.Errorf("User %s khong ton tai", req.Id)
	}
	log.Printf("Get user: %s", req.Id)

	return user, nil
}

func (s *userServer) ListUsers(
	req *pb.UserListRequest,
	stream pb.UserService_ListUsersServer,
) error {
	count := 0
	for _, user := range s.users {
		if int32(count) >= req.Limit {
			break
		}

		// Gui tung user qua 1 stream
		if err := stream.Send(user); err != nil {
			return err
		}
		count++

		// Gia lap delay giua cac message
		time.Sleep(500 * time.Millisecond)
	}
	return nil
}

// UploadFile client gui data qua stream thay vi server
func (s *userServer) UploadFile(
	stream pb.UserService_UploadFileServer,
) error {
	var (
		totalBytes int32
		filename   string
		allData    []byte
	)

	// lap nhan tung chunk du lieu tu client
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			// Client đã gửi xong — bây giờ mới xử lý và response
			break
		}
		if err != nil {
			return fmt.Errorf("Loi nhan chunk: %w", err)
		}

		// Tich luy data va thong tin
		filename = chunk.Filename
		allData = append(allData, chunk.Data...)
		totalBytes += int32(len(chunk.Data))

		log.Printf("Nhận chunk #%d của file %s (%d bytes)", chunk.Seq, chunk.Filename, len(chunk.Data))
	}

	// Xử lý xong → gửi 1 response duy nhất
	fileID := fmt.Sprintf("file-%d", time.Now().Unix())
	log.Printf("Upload hoàn tất: %s — tổng %d bytes", filename, totalBytes)
	return stream.SendAndClose(&pb.UploadResponse{
		FileId:     fileID,
		TotalBytes: totalBytes,
		Message:    fmt.Sprintf("Upload %s thành công", filename),
	})
}

// Chat là một ví dụ về Bidirectional Streaming RPC
func (s *userServer) Chat(
	stream pb.UserService_ChatServer, // ← vừa Recv vừa Send
) error {
	log.Println("Client kết nối vào chat")

	for {
		// Nhận message từ client
		msg, err := stream.Recv()
		if err == io.EOF {
			log.Println("Client đóng kết nối")
			return nil
		}
		if err != nil {
			return fmt.Errorf("lỗi nhận message: %v", err)
		}

		log.Printf("[%s]: %s", msg.User, msg.Text)

		// Server xử lý và push response ngay lập tức
		// (thực tế: broadcast cho các client khác, ở đây echo lại)
		reply := &pb.ChatMessage{
			User:   "server",
			Text:   fmt.Sprintf("Echo từ server: %s", msg.Text),
			SentAt: time.Now().Unix(),
		}

		if err := stream.Send(reply); err != nil {
			return fmt.Errorf("lỗi gửi reply: %v", err)
		}
	}
}

// ── 3. Khởi động server ──────────────────────────────

func main() {
	// mo TCP port
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatal("Khong the listen port: %w", err)
	}

	// Tao gRPC server
	grpcServer := grpc.NewServer()

	// Dang ky service implementation vao server
	pb.RegisterUserServiceServer(grpcServer, &userServer{
		users: make(map[string]*pb.CreateUserResponse),
	})

	log.Printf("gRPC server dang chay tai :50051")

	// Bat dau serve (Blocking)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatal("Loi serve: %w", err)
	}
}
