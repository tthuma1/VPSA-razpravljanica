// internal/dataplane/node.go
package dataplane

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
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
	prevNode    string // address of previous node in chain
	topologyMu  sync.RWMutex
	controlAddr string

	// Subscription management
	subscriptions map[string]*Subscription
	subMu         sync.RWMutex
	subCounter    int

	// Event broadcast
	eventListeners []chan *pb.MessageEvent
	eventMu        sync.RWMutex

	// Pending writes (Head only) and Unacknowledged writes (All nodes except Tail)
	// Map sequence number to:
	// - Head: channel for waiting client
	// - Middle: nil (just tracking existence for read consistency)
	pendingWrites map[int64]chan error
	pendingMu     sync.RWMutex

	// Syncing state
	syncing bool
	syncMu  sync.RWMutex

	// Replication queue ensures writes are forwarded in order
	replQueue chan *pb.WriteOp

	// Ack queue ensures acknowledgements are forwarded in order
	ackQueue chan int64
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
		pendingWrites: make(map[int64]chan error),
		syncing:       false,
		replQueue:     make(chan *pb.WriteOp, 1000),
		ackQueue:      make(chan int64, 1000),
	}

	go node.runReplicationWorker()
	go node.runAckWorker()

	return node, nil
}

func (n *Node) SetRole(role NodeRole, nextNode, prevNode string) {
	n.topologyMu.Lock()
	defer n.topologyMu.Unlock()
	if n.role == role && n.nextNode == nextNode && n.prevNode == prevNode {
		return
	}
	n.role = role
	n.nextNode = nextNode
	n.prevNode = prevNode
	log.Printf("Node %s role set to %v, next node: %s, prev node: %s", n.nodeID, role, nextNode, prevNode)
}

func (n *Node) checkSyncing() error {
	n.syncMu.RLock()
	defer n.syncMu.RUnlock()
	if n.syncing {
		return fmt.Errorf("node is syncing")
	}
	return nil
}

// Write operations (Head only)
func (n *Node) CreateUser(ctx context.Context, req *pb.CreateUserRequest) (*pb.User, error) {
	if err := n.checkSyncing(); err != nil {
		return nil, err
	}
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

		seq, err := n.storage.LogOperation(&pb.WriteOp{
			Operation: &pb.WriteOp_CreateUser{CreateUser: req},
		})
		if err != nil {
			return nil, err
		}

		errChan := make(chan error, 1)
		n.pendingMu.Lock()
		n.pendingWrites[seq] = errChan
		n.pendingMu.Unlock()

		err = n.replicateWrite(ctx, &pb.WriteOp{
			Operation: &pb.WriteOp_CreateUser{CreateUser: req},
			Sequence:  seq,
		})
		if err != nil {
			n.pendingMu.Lock()
			delete(n.pendingWrites, seq)
			n.pendingMu.Unlock()
			return nil, err
		}

		// Wait for acknowledgement
		select {
		case err := <-errChan:
			if err != nil {
				return nil, err
			}
		case <-ctx.Done():
			n.pendingMu.Lock()
			delete(n.pendingWrites, seq)
			n.pendingMu.Unlock()
			return nil, ctx.Err()
		}
	}

	return user, nil
}

func (n *Node) Login(ctx context.Context, req *pb.LoginRequest) (*pb.User, error) {
	if err := n.checkSyncing(); err != nil {
		return nil, err
	}
	// Login is a read operation but involves password verification.
	// Check if we have unacknowledged writes. If so, forward to tail.
	if n.role != RoleTail && n.hasPendingWrites() {
		return n.forwardLoginToTail(ctx, req)
	}

	user, err := n.storage.VerifyUser(req.Name, req.Password)
	if err != nil {
		return nil, err
	}

	return user, nil
}

func (n *Node) GetUser(ctx context.Context, req *pb.GetUserRequest) (*pb.User, error) {
	if err := n.checkSyncing(); err != nil {
		return nil, err
	}
	// Check if we have unacknowledged writes. If so, forward to tail.
	if n.role != RoleTail && n.hasPendingWrites() {
		return n.forwardGetUserToTail(ctx, req)
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
	if n.role != RoleTail && n.hasPendingWrites() {
		return n.forwardGetUserByIDToTail(ctx, req)
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
	if err := n.checkSyncing(); err != nil {
		return nil, err
	}
	if n.role != RoleHead {
		return nil, fmt.Errorf("not the head node")
	}

	topic, err := n.storage.CreateTopic(req.Name)
	if err != nil {
		return nil, err
	}

	if n.nextNode != "" {
		seq, err := n.storage.LogOperation(&pb.WriteOp{
			Operation: &pb.WriteOp_CreateTopic{CreateTopic: req},
		})
		if err != nil {
			return nil, err
		}

		errChan := make(chan error, 1)
		n.pendingMu.Lock()
		n.pendingWrites[seq] = errChan
		n.pendingMu.Unlock()

		err = n.replicateWrite(ctx, &pb.WriteOp{
			Operation: &pb.WriteOp_CreateTopic{CreateTopic: req},
			Sequence:  seq,
		})
		if err != nil {
			n.pendingMu.Lock()
			delete(n.pendingWrites, seq)
			n.pendingMu.Unlock()
			return nil, err
		}

		// Wait for acknowledgement
		select {
		case err := <-errChan:
			if err != nil {
				return nil, err
			}
		case <-ctx.Done():
			n.pendingMu.Lock()
			delete(n.pendingWrites, seq)
			n.pendingMu.Unlock()
			return nil, ctx.Err()
		}
	}

	return topic, nil
}

func (n *Node) GetTopic(ctx context.Context, req *pb.GetTopicRequest) (*pb.Topic, error) {
	if err := n.checkSyncing(); err != nil {
		return nil, err
	}
	// Check if we have unacknowledged writes. If so, forward to tail.
	if n.role != RoleTail && n.hasPendingWrites() {
		return n.forwardGetTopicToTail(ctx, req)
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
	if err := n.checkSyncing(); err != nil {
		return nil, err
	}
	if n.role != RoleHead {
		return nil, fmt.Errorf("not the head node")
	}

	msg, err := n.storage.PostMessage(req.TopicId, req.UserId, req.Text)
	if err != nil {
		return nil, err
	}

	seq, err := n.storage.LogOperation(&pb.WriteOp{
		Operation: &pb.WriteOp_PostMessage{PostMessage: req},
	})
	if err != nil {
		return nil, err
	}

	event := &pb.MessageEvent{
		SequenceNumber: seq,
		Op:             pb.OpType_OP_POST,
		Message:        msg,
		EventAt:        timestamppb.Now(),
	}

	// Broadcast event
	n.broadcastEvent(event)

	if n.nextNode != "" {
		go n.notifyEvent(event)

		errChan := make(chan error, 1)
		n.pendingMu.Lock()
		n.pendingWrites[seq] = errChan
		n.pendingMu.Unlock()

		err := n.replicateWrite(ctx, &pb.WriteOp{
			Operation: &pb.WriteOp_PostMessage{PostMessage: req},
			Sequence:  seq,
		})
		if err != nil {
			n.pendingMu.Lock()
			delete(n.pendingWrites, seq)
			n.pendingMu.Unlock()
			return nil, err
		}

		// Wait for acknowledgement
		select {
		case err := <-errChan:
			if err != nil {
				return nil, err
			}
		case <-ctx.Done():
			n.pendingMu.Lock()
			delete(n.pendingWrites, seq)
			n.pendingMu.Unlock()
			return nil, ctx.Err()
		}
	}

	return msg, nil
}

func (n *Node) LikeMessage(ctx context.Context, req *pb.LikeMessageRequest) (*pb.Message, error) {
	if err := n.checkSyncing(); err != nil {
		return nil, err
	}
	if n.role != RoleHead {
		return nil, fmt.Errorf("not the head node")
	}

	msg, err := n.storage.LikeMessage(req.TopicId, req.MessageId, req.UserId)
	if err != nil {
		return nil, err
	}

	seq, err := n.storage.LogOperation(&pb.WriteOp{
		Operation: &pb.WriteOp_LikeMessage{LikeMessage: req},
	})
	if err != nil {
		return nil, err
	}

	event := &pb.MessageEvent{
		SequenceNumber: seq,
		Op:             pb.OpType_OP_LIKE,
		Message:        msg,
		EventAt:        timestamppb.Now(),
	}

	n.broadcastEvent(event)

	if n.nextNode != "" {
		go n.notifyEvent(event)

		errChan := make(chan error, 1)
		n.pendingMu.Lock()
		n.pendingWrites[seq] = errChan
		n.pendingMu.Unlock()

		err := n.replicateWrite(ctx, &pb.WriteOp{
			Operation: &pb.WriteOp_LikeMessage{LikeMessage: req},
			Sequence:  seq,
		})
		if err != nil {
			n.pendingMu.Lock()
			delete(n.pendingWrites, seq)
			n.pendingMu.Unlock()
			return nil, err
		}

		// Wait for acknowledgement
		select {
		case err := <-errChan:
			if err != nil {
				return nil, err
			}
		case <-ctx.Done():
			n.pendingMu.Lock()
			delete(n.pendingWrites, seq)
			n.pendingMu.Unlock()
			return nil, ctx.Err()
		}
	}

	return msg, nil
}

// Read operations
func (n *Node) ListTopics(ctx context.Context, _ *emptypb.Empty) (*pb.ListTopicsResponse, error) {
	if err := n.checkSyncing(); err != nil {
		return nil, err
	}
	// Check if we have unacknowledged writes. If so, forward to tail.
	if n.role != RoleTail && n.hasPendingWrites() {
		return n.forwardListTopicsToTail(ctx)
	}

	topics, err := n.storage.ListTopics()
	if err != nil {
		return nil, err
	}

	return &pb.ListTopicsResponse{Topics: topics}, nil
}

func (n *Node) GetMessages(ctx context.Context, req *pb.GetMessagesRequest) (*pb.GetMessagesResponse, error) {
	if err := n.checkSyncing(); err != nil {
		return nil, err
	}
	// Check if we have unacknowledged writes. If so, forward to tail.
	if n.role != RoleTail && n.hasPendingWrites() {
		return n.forwardGetMessagesToTail(ctx, req)
	}

	messages, err := n.storage.GetMessages(req.TopicId, req.FromMessageId, req.Limit)
	if err != nil {
		return nil, err
	}

	return &pb.GetMessagesResponse{Messages: messages}, nil
}

func (n *Node) GetMessagesByUser(ctx context.Context, req *pb.GetMessagesByUserRequest) (*pb.GetMessagesResponse, error) {
	if err := n.checkSyncing(); err != nil {
		return nil, err
	}
	// Check if we have unacknowledged writes. If so, forward to tail.
	if n.role != RoleTail && n.hasPendingWrites() {
		return n.forwardGetMessagesByUserToTail(ctx, req)
	}

	messages, err := n.storage.GetMessagesByUser(req.TopicId, req.UserName, req.Limit)
	if err != nil {
		return nil, err
	}

	return &pb.GetMessagesResponse{Messages: messages}, nil
}

// Subscription handling
func (n *Node) GetSubscriptionNode(ctx context.Context, req *pb.SubscriptionNodeRequest) (*pb.SubscriptionNodeResponse, error) {
	if err := n.checkSyncing(); err != nil {
		return nil, err
	}
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
	if err := n.checkSyncing(); err != nil {
		return err
	}
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

	// Mark as unacknowledged before processing
	if n.role != RoleTail {
		n.pendingMu.Lock()
		if _, exists := n.pendingWrites[op.Sequence]; !exists {
			n.pendingWrites[op.Sequence] = nil
		}
		n.pendingMu.Unlock()
	}

	if err := n.applyWriteOp(op); err != nil {
		return nil, err
	}

	// Forward to next node
	if n.nextNode != "" && n.role != RoleTail {
		n.replQueue <- op
	} else if n.role == RoleTail {
		// If we are the tail, we acknowledge the write to the previous node
		n.ackQueue <- op.Sequence
		// Also update our own last acked sequence
		if err := n.storage.UpdateLastAcked(op.Sequence); err != nil {
			log.Printf("Failed to update last acked: %v", err)
		}
	}

	return &emptypb.Empty{}, nil
}

func (n *Node) applyWriteOp(op *pb.WriteOp) error {
	// Log the operation first (idempotent)
	if _, err := n.storage.LogOperation(op); err != nil {
		return err
	}

	switch o := op.Operation.(type) {
	case *pb.WriteOp_CreateUser:
		_, err := n.storage.CreateUser(o.CreateUser.Name, o.CreateUser.Password, o.CreateUser.Salt)
		if err != nil {
			// If user already exists, it might be due to replay. Check if it exists.
			if _, errGet := n.storage.GetUserByName(o.CreateUser.Name); errGet == nil {
				return nil
			}
			return err
		}
	case *pb.WriteOp_CreateTopic:
		_, err := n.storage.CreateTopic(o.CreateTopic.Name)
		if err != nil {
			if _, errGet := n.storage.GetTopicByName(o.CreateTopic.Name); errGet == nil {
				return nil
			}
			return err
		}
	case *pb.WriteOp_PostMessage:
		_, err := n.storage.PostMessage(o.PostMessage.TopicId, o.PostMessage.UserId, o.PostMessage.Text)
		if err != nil {
			return err
		}
	case *pb.WriteOp_LikeMessage:
		_, err := n.storage.LikeMessage(o.LikeMessage.TopicId, o.LikeMessage.MessageId, o.LikeMessage.UserId)
		if err != nil {
			return err
		}
	}
	return nil
}

func (n *Node) NotifyEvent(ctx context.Context, event *pb.MessageEvent) (*emptypb.Empty, error) {
	n.broadcastEvent(event)

	if n.nextNode != "" && n.role != RoleTail {
		go n.notifyEvent(event)
	}

	return &emptypb.Empty{}, nil
}

func (n *Node) AcknowledgeWrite(ctx context.Context, req *pb.AcknowledgeRequest) (*emptypb.Empty, error) {
	// Persist acknowledgement
	if err := n.storage.UpdateLastAcked(req.Sequence); err != nil {
		log.Printf("Failed to persist acknowledgement for seq %d: %v", req.Sequence, err)
	}

	n.pendingMu.Lock()
	ch, exists := n.pendingWrites[req.Sequence]
	delete(n.pendingWrites, req.Sequence)
	n.pendingMu.Unlock()

	if n.role == RoleHead {
		if exists && ch != nil {
			ch <- nil
		}
	} else {
		// Propagate acknowledgement to previous node
		n.ackQueue <- req.Sequence
	}
	return &emptypb.Empty{}, nil
}

func (n *Node) GetLastAcked(ctx context.Context, _ *emptypb.Empty) (*pb.GetLastAckedResponse, error) {
	seq, err := n.storage.GetLastAcked()
	if err != nil {
		return nil, err
	}
	return &pb.GetLastAckedResponse{Sequence: seq}, nil
}

func (n *Node) runReplicationWorker() {
	for op := range n.replQueue {
		// Forward writes sequentially to preserve order
		if err := n.replicateWrite(context.Background(), op); err != nil {
			log.Printf("Failed to replicate write (seq %d): %v", op.Sequence, err)
		}
	}
}

func (n *Node) runAckWorker() {
	for seq := range n.ackQueue {
		if err := n.acknowledgeWrite(context.Background(), seq); err != nil {
			log.Printf("Failed to acknowledge write %d: %v", seq, err)
		}
	}
}

func (n *Node) replicateWrite(ctx context.Context, op *pb.WriteOp) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		n.topologyMu.RLock()
		next := n.nextNode
		n.topologyMu.RUnlock()

		if next == "" {
			return nil
		}

		err := func() error {
			conn, err := grpc.NewClient(next, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			defer conn.Close()

			client := pb.NewReplicationClient(conn)
			reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			_, err = client.ReplicateWrite(reqCtx, &pb.ReplicationRequest{Op: op})
			return err
		}()

		if err == nil {
			return nil
		}

		log.Printf("Failed to replicate write (seq %d) to %s: %v. Retrying...", op.Sequence, next, err)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
			// retry
		}
	}
}

func (n *Node) acknowledgeWrite(ctx context.Context, sequence int64) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		n.topologyMu.RLock()
		prev := n.prevNode
		n.topologyMu.RUnlock()

		if prev == "" {
			return nil
		}

		err := func() error {
			conn, err := grpc.NewClient(prev, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			defer conn.Close()

			client := pb.NewReplicationClient(conn)
			reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			_, err = client.AcknowledgeWrite(reqCtx, &pb.AcknowledgeRequest{Sequence: sequence})
			return err
		}()

		if err == nil {
			return nil
		}

		log.Printf("Failed to acknowledge write (seq %d) to %s: %v. Retrying...", sequence, prev, err)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
			// retry
		}
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

func (n *Node) StreamLog(req *pb.StreamLogRequest, stream pb.Replication_StreamLogServer) error {
	ops, err := n.storage.GetOperations(req.FromSequence)
	if err != nil {
		return err
	}
	for _, op := range ops {
		if err := stream.Send(op); err != nil {
			return err
		}
	}
	return nil
}

func (n *Node) SyncWithTail(ctx context.Context) error {
	n.syncMu.Lock()
	n.syncing = true
	n.syncMu.Unlock()

	defer func() {
		n.syncMu.Lock()
		n.syncing = false
		n.syncMu.Unlock()
	}()

	// Get tail node from control plane
	cpConn, err := grpc.NewClient(n.controlAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to control plane: %v", err)
	}
	defer cpConn.Close()
	cpClient := pb.NewControlPlaneClient(cpConn)

	state, err := cpClient.GetClusterState(ctx, &emptypb.Empty{})
	if err != nil {
		return fmt.Errorf("failed to get cluster state: %v", err)
	}

	if state.Tail == nil {
		log.Printf("No tail node found, skipping sync")
		_, err = cpClient.ConfirmSynced(ctx, &pb.ConfirmSyncedRequest{NodeId: n.nodeID})
		return err
	}

	tailAddr := state.Tail.Address
	log.Printf("Starting sync with tail node %s", tailAddr)

	conn, err := grpc.NewClient(tailAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	client := pb.NewReplicationClient(conn)

	// Get local last sequence
	lastSeq := n.storage.GetLastSequence()
	log.Printf("Local last sequence: %d", lastSeq)

	// Stream Log
	stream, err := client.StreamLog(ctx, &pb.StreamLogRequest{FromSequence: lastSeq + 1})
	if err != nil {
		return fmt.Errorf("failed to start log stream: %v", err)
	}

	log.Printf("Streaming log from sequence %d", lastSeq+1)

	for {
		op, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("stream error: %v", err)
		}
		// Apply op
		log.Printf("Applying %d, %s", op.Sequence, op.Operation)
		if err := n.applyWriteOp(op); err != nil {
			return fmt.Errorf("failed to apply op %d: %v", op.Sequence, err)
		}

		// Update last acked sequence since we received it from tail
		if err := n.storage.UpdateLastAcked(op.Sequence); err != nil {
			log.Printf("Failed to update last acked during sync: %v", err)
		}
	}

	log.Printf("Sync completed")

	// Confirm Synced
	_, err = cpClient.ConfirmSynced(ctx, &pb.ConfirmSyncedRequest{NodeId: n.nodeID})
	if err != nil {
		return fmt.Errorf("failed to confirm synced: %v", err)
	}

	return nil
}

func (n *Node) Close() {
	n.storage.Close()
}

func (n *Node) NodeID() string {
	return n.nodeID
}

func (n *Node) hasPendingWrites() bool {
	n.pendingMu.RLock()
	defer n.pendingMu.RUnlock()
	return len(n.pendingWrites) > 0
}

// Forwarding methods
func (n *Node) forwardLoginToTail(ctx context.Context, req *pb.LoginRequest) (*pb.User, error) {
	client, conn, err := n.getTailClient()
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return client.Login(ctx, req)
}

func (n *Node) forwardGetUserToTail(ctx context.Context, req *pb.GetUserRequest) (*pb.User, error) {
	client, conn, err := n.getTailClient()
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return client.GetUser(ctx, req)
}

func (n *Node) forwardGetTopicToTail(ctx context.Context, req *pb.GetTopicRequest) (*pb.Topic, error) {
	client, conn, err := n.getTailClient()
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return client.GetTopic(ctx, req)
}

func (n *Node) forwardListTopicsToTail(ctx context.Context) (*pb.ListTopicsResponse, error) {
	client, conn, err := n.getTailClient()
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return client.ListTopics(ctx, &emptypb.Empty{})
}

func (n *Node) forwardGetMessagesToTail(ctx context.Context, req *pb.GetMessagesRequest) (*pb.GetMessagesResponse, error) {
	client, conn, err := n.getTailClient()
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return client.GetMessages(ctx, req)
}

func (n *Node) forwardGetMessagesByUserToTail(ctx context.Context, req *pb.GetMessagesByUserRequest) (*pb.GetMessagesResponse, error) {
	client, conn, err := n.getTailClient()
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return client.GetMessagesByUser(ctx, req)
}

func (n *Node) forwardGetUserByIDToTail(ctx context.Context, req *pb.GetUserByIdRequest) (*pb.User, error) {
	client, conn, err := n.getTailClient()
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return client.GetUserById(ctx, req)
}

func (n *Node) getTailClient() (pb.MessageBoardClient, *grpc.ClientConn, error) {
	// We need to find the tail node. We can ask the control plane.
	conn, err := grpc.NewClient(n.controlAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to control plane: %v", err)
	}
	// Don't close control plane conn here, we need it for the request

	cpClient := pb.NewControlPlaneClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	state, err := cpClient.GetClusterState(ctx, &emptypb.Empty{})
	cancel()
	conn.Close() // Close control plane connection

	if err != nil {
		return nil, nil, fmt.Errorf("failed to get cluster state: %v", err)
	}

	if state.Tail == nil {
		return nil, nil, fmt.Errorf("tail node not found")
	}

	tailConn, err := grpc.NewClient(state.Tail.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to tail node: %v", err)
	}

	return pb.NewMessageBoardClient(tailConn), tailConn, nil
}
