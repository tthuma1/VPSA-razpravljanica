// internal/dataplane/node.go
package dataplane

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"sync"
	"time"

	pb "messageboard/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type NodeRole int

const (
	RoleHead NodeRole = iota
	RoleTail
	RoleMiddle
)

type Node struct {
	pb.UnimplementedMessageBoardServer
	pb.UnimplementedReplicationServer

	nodeID      string
	address     string
	storage     *Storage
	role        NodeRole
	nextNode    string // address of next node in chain
	controlAddr string

	// Subscription management
	subscriptions map[string]*Subscription
	subMu         sync.RWMutex
	subCounter    int

	// Event broadcast
	eventListeners []chan *pb.MessageEvent
	eventMu        sync.RWMutex
}

type Subscription struct {
	UserID        int64
	TopicIDs      []int64
	FromMessageID int64
	EventChan     chan *pb.MessageEvent
}

func NewNode(nodeID, address, dbPath, controlAddr string) (*Node, error) {
	storage, err := NewStorage(dbPath)
	if err != nil {
		return nil, err
	}

	node := &Node{
		nodeID:        nodeID,
		address:       address,
		storage:       storage,
		subscriptions: make(map[string]*Subscription),
		controlAddr:   controlAddr,
	}

	return node, nil
}

func (n *Node) SetRole(role NodeRole, nextNode string) {
	n.role = role
	n.nextNode = nextNode
	log.Printf("Node %s role set to %v, next node: %s", n.nodeID, role, nextNode)
}

// Write operations (Head only)
func (n *Node) CreateUser(ctx context.Context, req *pb.CreateUserRequest) (*pb.User, error) {
	if n.role != RoleHead {
		return nil, fmt.Errorf("not the head node")
	}

	user, err := n.storage.CreateUser(req.Name, req.Password, req.Salt)
	if err != nil {
		return nil, err
	}

	// Replicate to next node
	if n.nextNode != "" {
		// Update request with generated salt if it was empty
		req.Salt = user.Salt

		go n.replicateWrite(&pb.WriteOp{
			Operation: &pb.WriteOp_CreateUser{CreateUser: req},
			Sequence:  n.storage.NextSequence(),
		})
	}

	return user, nil
}

func (n *Node) Login(ctx context.Context, req *pb.LoginRequest) (*pb.User, error) {
	// Login is a read operation but involves password verification.
	// In chain replication, reads go to Tail.
	if n.role != RoleTail {
		return nil, fmt.Errorf("not the tail node")
	}

	user, err := n.storage.VerifyUser(req.Name, req.Password)
	if err != nil {
		return nil, err
	}

	return user, nil
}

func (n *Node) GetUser(ctx context.Context, req *pb.GetUserRequest) (*pb.User, error) {
	// Reads can be served by Tail (strong consistency) or Head (read-your-writes, though not strictly guaranteed here without more logic).
	// For simplicity and consistency with other reads, let's enforce Tail.
	// However, for login, we might want to check Head if we just created the user.
	// But standard chain replication says reads go to Tail.
	if n.role != RoleTail {
		return nil, fmt.Errorf("not the tail node")
	}

	user, err := n.storage.GetUserByName(req.Name)
	if err != nil {
		return nil, err
	}
	if user == nil {
		return nil, fmt.Errorf("user not found")
	}

	return user, nil
}

func (n *Node) GetUserById(ctx context.Context, req *pb.GetUserByIdRequest) (*pb.User, error) {
	if n.role != RoleTail {
		return nil, fmt.Errorf("not the tail node")
	}

	user, err := n.storage.GetUserById(req.Id)
	if err != nil {
		return nil, err
	}
	if user == nil {
		return nil, fmt.Errorf("user not found")
	}

	return user, nil
}

func (n *Node) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.Topic, error) {
	if n.role != RoleHead {
		return nil, fmt.Errorf("not the head node")
	}

	topic, err := n.storage.CreateTopic(req.Name)
	if err != nil {
		return nil, err
	}

	if n.nextNode != "" {
		go n.replicateWrite(&pb.WriteOp{
			Operation: &pb.WriteOp_CreateTopic{CreateTopic: req},
			Sequence:  n.storage.NextSequence(),
		})
	}

	return topic, nil
}

func (n *Node) GetTopic(ctx context.Context, req *pb.GetTopicRequest) (*pb.Topic, error) {
	// Reads go to Tail
	if n.role != RoleTail {
		return nil, fmt.Errorf("not the tail node")
	}

	topic, err := n.storage.GetTopicByName(req.Name)
	if err != nil {
		return nil, err
	}
	if topic == nil {
		return nil, fmt.Errorf("topic not found")
	}

	return topic, nil
}

func (n *Node) PostMessage(ctx context.Context, req *pb.PostMessageRequest) (*pb.Message, error) {
	if n.role != RoleHead {
		return nil, fmt.Errorf("not the head node")
	}

	msg, err := n.storage.PostMessage(req.TopicId, req.UserId, req.Text)
	if err != nil {
		return nil, err
	}

	seq := n.storage.NextSequence()
	event := &pb.MessageEvent{
		SequenceNumber: seq,
		Op:             pb.OpType_OP_POST,
		Message:        msg,
		EventAt:        timestamppb.Now(),
	}

	// Broadcast event
	n.broadcastEvent(event)

	if n.nextNode != "" {
		go n.replicateWrite(&pb.WriteOp{
			Operation: &pb.WriteOp_PostMessage{PostMessage: req},
			Sequence:  seq,
		})
		go n.notifyEvent(event)
	}

	return msg, nil
}

func (n *Node) LikeMessage(ctx context.Context, req *pb.LikeMessageRequest) (*pb.Message, error) {
	if n.role != RoleHead {
		return nil, fmt.Errorf("not the head node")
	}

	msg, err := n.storage.LikeMessage(req.TopicId, req.MessageId, req.UserId)
	if err != nil {
		return nil, err
	}

	seq := n.storage.NextSequence()
	event := &pb.MessageEvent{
		SequenceNumber: seq,
		Op:             pb.OpType_OP_LIKE,
		Message:        msg,
		EventAt:        timestamppb.Now(),
	}

	n.broadcastEvent(event)

	if n.nextNode != "" {
		go n.replicateWrite(&pb.WriteOp{
			Operation: &pb.WriteOp_LikeMessage{LikeMessage: req},
			Sequence:  seq,
		})
		go n.notifyEvent(event)
	}

	return msg, nil
}

// Read operations (Tail only)
func (n *Node) ListTopics(ctx context.Context, _ *emptypb.Empty) (*pb.ListTopicsResponse, error) {
	if n.role != RoleTail {
		return nil, fmt.Errorf("not the tail node")
	}

	topics, err := n.storage.ListTopics()
	if err != nil {
		return nil, err
	}

	return &pb.ListTopicsResponse{Topics: topics}, nil
}

func (n *Node) GetMessages(ctx context.Context, req *pb.GetMessagesRequest) (*pb.GetMessagesResponse, error) {
	if n.role != RoleTail {
		return nil, fmt.Errorf("not the tail node")
	}

	messages, err := n.storage.GetMessages(req.TopicId, req.FromMessageId, req.Limit)
	if err != nil {
		return nil, err
	}

	return &pb.GetMessagesResponse{Messages: messages}, nil
}

func (n *Node) GetMessagesByUser(ctx context.Context, req *pb.GetMessagesByUserRequest) (*pb.GetMessagesResponse, error) {
	if n.role != RoleTail {
		return nil, fmt.Errorf("not the tail node")
	}

	messages, err := n.storage.GetMessagesByUser(req.TopicId, req.UserName, req.Limit)
	if err != nil {
		return nil, err
	}

	return &pb.GetMessagesResponse{Messages: messages}, nil
}

// Subscription handling
func (n *Node) GetSubscriptionNode(ctx context.Context, req *pb.SubscriptionNodeRequest) (*pb.SubscriptionNodeResponse, error) {
	if n.role != RoleHead {
		return nil, fmt.Errorf("not the head node")
	}

	token := generateToken()

	// Simple round-robin load balancing - assign to self for now
	return &pb.SubscriptionNodeResponse{
		SubscribeToken: token,
		Node: &pb.NodeInfo{
			NodeId:  n.nodeID,
			Address: n.address,
		},
	}, nil
}

func (n *Node) SubscribeTopic(req *pb.SubscribeTopicRequest, stream pb.MessageBoard_SubscribeTopicServer) error {
	sub := &Subscription{
		UserID:        req.UserId,
		TopicIDs:      req.TopicId,
		FromMessageID: req.FromMessageId,
		EventChan:     make(chan *pb.MessageEvent, 100),
	}

	token := req.SubscribeToken
	n.subMu.Lock()
	n.subscriptions[token] = sub
	n.subMu.Unlock()

	defer func() {
		n.subMu.Lock()
		delete(n.subscriptions, token)
		n.subMu.Unlock()
		close(sub.EventChan)
	}()

	// Send historical messages
	for _, topicID := range sub.TopicIDs {
		messages, err := n.storage.GetMessages(topicID, sub.FromMessageID, 1000)
		if err != nil {
			continue
		}

		for _, msg := range messages {
			event := &pb.MessageEvent{
				SequenceNumber: msg.Id,
				Op:             pb.OpType_OP_POST,
				Message:        msg,
				EventAt:        msg.CreatedAt,
			}
			if err := stream.Send(event); err != nil {
				return err
			}
		}
	}

	// Stream new events
	for event := range sub.EventChan {
		if err := stream.Send(event); err != nil {
			return err
		}
	}

	return nil
}

// Replication
func (n *Node) ReplicateWrite(ctx context.Context, req *pb.ReplicationRequest) (*emptypb.Empty, error) {
	op := req.Op

	switch o := op.Operation.(type) {
	case *pb.WriteOp_CreateUser:
		_, err := n.storage.CreateUser(o.CreateUser.Name, o.CreateUser.Password, o.CreateUser.Salt)
		if err != nil {
			return nil, err
		}
	case *pb.WriteOp_CreateTopic:
		_, err := n.storage.CreateTopic(o.CreateTopic.Name)
		if err != nil {
			return nil, err
		}
	case *pb.WriteOp_PostMessage:
		_, err := n.storage.PostMessage(o.PostMessage.TopicId, o.PostMessage.UserId, o.PostMessage.Text)
		if err != nil {
			return nil, err
		}
	case *pb.WriteOp_LikeMessage:
		_, err := n.storage.LikeMessage(o.LikeMessage.TopicId, o.LikeMessage.MessageId, o.LikeMessage.UserId)
		if err != nil {
			return nil, err
		}
	}

	// Forward to next node
	if n.nextNode != "" && n.role != RoleTail {
		go n.replicateWrite(op)
	}

	return &emptypb.Empty{}, nil
}

func (n *Node) NotifyEvent(ctx context.Context, event *pb.MessageEvent) (*emptypb.Empty, error) {
	n.broadcastEvent(event)

	if n.nextNode != "" && n.role != RoleTail {
		go n.notifyEvent(event)
	}

	return &emptypb.Empty{}, nil
}

func (n *Node) replicateWrite(op *pb.WriteOp) {
	if n.nextNode == "" {
		return
	}

	conn, err := grpc.NewClient(n.nextNode, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Failed to connect to next node: %v", err)
		return
	}
	defer conn.Close()

	client := pb.NewReplicationClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = client.ReplicateWrite(ctx, &pb.ReplicationRequest{Op: op})
	if err != nil {
		log.Printf("Failed to replicate write: %v", err)
	}
}

func (n *Node) notifyEvent(event *pb.MessageEvent) {
	if n.nextNode == "" {
		return
	}

	conn, err := grpc.NewClient(n.nextNode, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return
	}
	defer conn.Close()

	client := pb.NewReplicationClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client.NotifyEvent(ctx, event)
}

func (n *Node) broadcastEvent(event *pb.MessageEvent) {
	n.subMu.RLock()
	defer n.subMu.RUnlock()

	for _, sub := range n.subscriptions {
		for _, topicID := range sub.TopicIDs {
			if topicID == event.Message.TopicId {
				select {
				case sub.EventChan <- event:
				default:
				}
				break
			}
		}
	}
}

func generateToken() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func (n *Node) SyncState(ctx context.Context, _ *emptypb.Empty) (*pb.StateSnapshot, error) {
	return &pb.StateSnapshot{
		LastSequence: n.storage.GetLastSequence(),
	}, nil
}

func (n *Node) Close() {
	n.storage.Close()
}

func (n *Node) NodeID() string {
	return n.nodeID
}
