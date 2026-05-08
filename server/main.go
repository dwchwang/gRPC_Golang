package main

import (
	"context"
	"fmt"
	"grpc-demo/pb"
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
		if int32(count) >= req.Limit{
			break
		}

		// Gui tung user qua 1 stream
		if err := stream.Send(user); err != nil {
			return err;
		}
		count++
		
		// Gia lap delay giua cac message 
		time.Sleep(500*time.Millisecond)
	}
	return nil
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
