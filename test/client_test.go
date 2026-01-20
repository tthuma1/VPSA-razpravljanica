package test

import (
	"context"
	"fmt"
	"messageboard/internal/client"
	pb "messageboard/proto"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

// MockControlPlane implements pb.ControlPlaneServer
type MockControlPlane struct {
	pb.UnimplementedControlPlaneServer
	chain []*pb.NodeInfo
}

func (m *MockControlPlane) GetChainState(ctx context.Context, _ *emptypb.Empty) (*pb.ChainStateResponse, error) {
	return &pb.ChainStateResponse{Chain: m.chain}, nil
}

// MockMessageBoard implements pb.MessageBoardServer
type MockMessageBoard struct {
	pb.UnimplementedMessageBoardServer
	users             map[string]*pb.User
	topics            map[string]*pb.Topic
	topicParticipants map[int64]map[int64]bool // topicId -> userId -> bool
	messages          []*pb.Message
	likes             map[int64]int32
	address           string
}

func NewMockMessageBoard(address string) *MockMessageBoard {
	return &MockMessageBoard{
		users:             make(map[string]*pb.User),
		topics:            make(map[string]*pb.Topic),
		topicParticipants: make(map[int64]map[int64]bool),
		messages:          make([]*pb.Message, 0),
		likes:             make(map[int64]int32),
		address:           address,
	}
}

func (m *MockMessageBoard) CreateUser(ctx context.Context, req *pb.CreateUserRequest) (*pb.User, error) {
	if _, exists := m.users[req.Name]; exists {
		return nil, fmt.Errorf("user exists")
	}
	user := &pb.User{
		Id:   int64(len(m.users) + 1),
		Name: req.Name,
	}
	m.users[req.Name] = user
	return user, nil
}

func (m *MockMessageBoard) Login(ctx context.Context, req *pb.LoginRequest) (*pb.User, error) {
	user, exists := m.users[req.Name]
	if !exists {
		return nil, fmt.Errorf("user not found")
	}
	return user, nil
}

func (m *MockMessageBoard) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.Topic, error) {
	if _, exists := m.topics[req.Name]; exists {
		return nil, fmt.Errorf("topic exists")
	}
	topic := &pb.Topic{
		Id:   int64(len(m.topics) + 1),
		Name: req.Name,
	}
	m.topics[req.Name] = topic

	// Auto-join creator
	if m.topicParticipants[topic.Id] == nil {
		m.topicParticipants[topic.Id] = make(map[int64]bool)
	}
	m.topicParticipants[topic.Id][req.UserId] = true

	return topic, nil
}

func (m *MockMessageBoard) GetTopic(ctx context.Context, req *pb.GetTopicRequest) (*pb.Topic, error) {
	topic, exists := m.topics[req.Name]
	if !exists {
		return nil, fmt.Errorf("topic not found")
	}
	return topic, nil
}

func (m *MockMessageBoard) ListTopics(ctx context.Context, req *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
	topics := make([]*pb.Topic, 0)
	for _, t := range m.topics {
		if participants, ok := m.topicParticipants[t.Id]; ok {
			if participants[req.UserId] {
				topics = append(topics, t)
			}
		}
	}
	return &pb.ListTopicsResponse{Topics: topics}, nil
}

func (m *MockMessageBoard) ListAllTopics(ctx context.Context, _ *emptypb.Empty) (*pb.ListTopicsResponse, error) {
	topics := make([]*pb.Topic, 0, len(m.topics))
	for _, t := range m.topics {
		topics = append(topics, t)
	}
	return &pb.ListTopicsResponse{Topics: topics}, nil
}

func (m *MockMessageBoard) ListJoinableTopics(ctx context.Context, req *pb.ListJoinableTopicsRequest) (*pb.ListTopicsResponse, error) {
	topics := make([]*pb.Topic, 0)
	for _, t := range m.topics {
		joined := false
		if participants, ok := m.topicParticipants[t.Id]; ok {
			if participants[req.UserId] {
				joined = true
			}
		}
		if !joined {
			topics = append(topics, t)
		}
	}
	return &pb.ListTopicsResponse{Topics: topics}, nil
}

func (m *MockMessageBoard) JoinTopic(ctx context.Context, req *pb.JoinTopicRequest) (*emptypb.Empty, error) {
	var topicId int64 = -1
	for _, t := range m.topics {
		if t.Id == req.TopicId {
			topicId = t.Id
			break
		}
	}

	if topicId == -1 {
		return nil, fmt.Errorf("topic not found")
	}

	if m.topicParticipants[req.TopicId] == nil {
		m.topicParticipants[req.TopicId] = make(map[int64]bool)
	}
	m.topicParticipants[req.TopicId][req.UserId] = true
	return &emptypb.Empty{}, nil
}

func (m *MockMessageBoard) PostMessage(ctx context.Context, req *pb.PostMessageRequest) (*pb.Message, error) {
	msg := &pb.Message{
		Id:      int64(len(m.messages) + 1),
		TopicId: req.TopicId,
		UserId:  req.UserId,
		Text:    req.Text,
		Likes:   0,
	}
	m.messages = append(m.messages, msg)
	return msg, nil
}

func (m *MockMessageBoard) LikeMessage(ctx context.Context, req *pb.LikeMessageRequest) (*pb.Message, error) {
	// Find message
	var msg *pb.Message
	for _, m := range m.messages {
		if m.Id == req.MessageId {
			msg = m
			break
		}
	}
	if msg == nil {
		return nil, fmt.Errorf("message not found")
	}

	if req.Like {
		msg.Likes++
	} else {
		msg.Likes--
	}
	return msg, nil
}

func (m *MockMessageBoard) GetUserById(ctx context.Context, req *pb.GetUserByIdRequest) (*pb.User, error) {
	for _, u := range m.users {
		if u.Id == req.Id {
			return u, nil
		}
	}
	return nil, fmt.Errorf("user not found")
}

func (m *MockMessageBoard) SubscribeTopic(req *pb.SubscribeTopicRequest, stream pb.MessageBoard_SubscribeTopicServer) error {
	// Send one event and close
	event := &pb.MessageEvent{
		Op: pb.OpType_OP_POST,
		Message: &pb.Message{
			Id:   100,
			Text: "streamed message",
		},
	}
	if err := stream.Send(event); err != nil {
		return err
	}
	return nil
}

func (m *MockMessageBoard) GetSubscriptionNode(ctx context.Context, req *pb.SubscriptionNodeRequest) (*pb.SubscriptionNodeResponse, error) {
	return &pb.SubscriptionNodeResponse{
		Node:           &pb.NodeInfo{Address: m.address},
		SubscribeToken: "token",
	}, nil
}

func startMockServer(t *testing.T) (string, *grpc.Server, *MockControlPlane, *MockMessageBoard) {
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()

	addr := lis.Addr().String()
	mockCP := &MockControlPlane{
		chain: []*pb.NodeInfo{{Address: addr}},
	}
	mockMB := NewMockMessageBoard(addr)

	pb.RegisterControlPlaneServer(s, mockCP)
	pb.RegisterMessageBoardServer(s, mockMB)

	go func() {
		if err := s.Serve(lis); err != nil {
			// t.Errorf("failed to serve: %v", err) // Can't call t.Errorf in goroutine
		}
	}()

	return addr, s, mockCP, mockMB
}

func TestClientOperations(t *testing.T) {
	addr, server, _, _ := startMockServer(t)
	defer server.Stop()

	c := client.New()
	defer c.Close()

	// Test Connect
	if err := c.Connect(addr); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	ctx := context.Background()

	// Test RegisterUser
	if err := c.RegisterUser(ctx, "testuser", "password"); err != nil {
		t.Fatalf("RegisterUser failed: %v", err)
	}
	if c.GetCurrentUserName() != "testuser" {
		t.Errorf("Expected username testuser, got %s", c.GetCurrentUserName())
	}

	// Test LoginUser
	if err := c.LoginUser(ctx, "testuser", "password"); err != nil {
		t.Fatalf("LoginUser failed: %v", err)
	}

	// Test CreateTopic
	if err := c.CreateTopic(ctx, "testtopic"); err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}

	// Test ListTopics
	topics, err := c.ListTopics(ctx)
	if err != nil {
		t.Fatalf("ListTopics failed: %v", err)
	}
	if len(topics) != 1 || topics[0].Name != "testtopic" {
		t.Errorf("Expected 1 topic 'testtopic', got %v", topics)
	}

	// Test PostMessage
	if err := c.PostMessage(ctx, "testtopic", "hello world"); err != nil {
		t.Fatalf("PostMessage failed: %v", err)
	}

	// Test SetMessageLike
	// We know message ID is 1 because it's the first message
	if err := c.SetMessageLike(ctx, "testtopic", 1, true); err != nil {
		t.Fatalf("SetMessageLike failed: %v", err)
	}

	// Test GetUserById
	user, err := c.GetUserById(ctx, c.GetCurrentUserId())
	if err != nil {
		t.Fatalf("GetUserById failed: %v", err)
	}
	if user.Name != "testuser" {
		t.Errorf("Expected user name testuser, got %s", user.Name)
	}
}

func TestClientTopologyRefresh(t *testing.T) {
	// Start two servers
	addr1, server1, cp1, _ := startMockServer(t)
	defer server1.Stop()

	addr2, server2, _, _ := startMockServer(t)
	defer server2.Stop()

	// Initial chain points to server1
	cp1.chain = []*pb.NodeInfo{{Address: addr1}}

	c := client.New()
	defer c.Close()

	if err := c.Connect(addr1); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	// Update chain to point to server2
	cp1.chain = []*pb.NodeInfo{{Address: addr2}}

	// Force refresh by calling RefreshTopology directly or simulating failure
	// Here we just call RefreshTopology
	if err := c.RefreshTopology(); err != nil {
		t.Fatalf("RefreshTopology failed: %v", err)
	}

	// Now client should be connected to server2 (head)
	// We can verify by checking internal state or making a call that only server2 handles correctly?
	// Since both are mocks, they behave same. But we can check if we can still make calls.
	ctx := context.Background()
	if err := c.RegisterUser(ctx, "user2", "pass"); err != nil {
		t.Fatalf("RegisterUser after refresh failed: %v", err)
	}
}

func TestSubscribe(t *testing.T) {
	addr, server, _, _ := startMockServer(t)
	defer server.Stop()

	c := client.New()
	defer c.Close()

	if err := c.Connect(addr); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	ctx := context.Background()
	if err := c.RegisterUser(ctx, "subuser", "pass"); err != nil {
		t.Fatalf("RegisterUser failed: %v", err)
	}
	if err := c.CreateTopic(ctx, "subtopic"); err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}

	// Get subscription node
	subResp, err := c.GetSubscriptionNode(ctx, []int64{1})
	if err != nil {
		t.Fatalf("GetSubscriptionNode failed: %v", err)
	}

	// Subscribe
	stream, err := c.SubscribeTopic(ctx, subResp.Node.Address, []int64{1}, 0, subResp.SubscribeToken)
	if err != nil {
		t.Fatalf("SubscribeTopic failed: %v", err)
	}

	// Receive event
	event, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv failed: %v", err)
	}
	if event.Message.Text != "streamed message" {
		t.Errorf("Expected message 'streamed message', got %s", event.Message.Text)
	}
}
