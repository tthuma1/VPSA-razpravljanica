package dataplane

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
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
	dbPath      string
	testing     bool
	storage     *Storage
	role        NodeRole
	nextNode    string // address of next node in chain
	prevNode    string // address of previous node in chain
	topologyMu  sync.RWMutex
	controlAddr string
	cpClient    pb.ControlPlaneClient // Client for control plane communication
	cpClientMu  sync.RWMutex

	// Subscription management
	subscriptions map[string]*Subscription
	subMu         sync.RWMutex

	// This map is used so that Head waits for ACK before sending a response to the client.
	pendingWrites map[int64]chan error
	pendingMu     sync.RWMutex

	// Syncing state
	syncing bool
	syncMu  sync.RWMutex

	// Replication queue ensures writes are forwarded in order
	replQueue chan *pb.WriteOp

	// Ack queue ensures acknowledgements are forwarded in order
	ackQueue chan int64

	// Round robin index for subscription load balancing
	rrIndex uint64
}

type Subscription struct {
	UserID        int64
	TopicIDs      []int64
	FromMessageID int64
	EventChan     chan *pb.MessageEvent
}

func NewNode(nodeID, address, dbPath, controlAddr string, testing bool) (*Node, error) {
	storage, err := NewStorage(dbPath)
	if err != nil {
		return nil, err
	}

	node := &Node{
		nodeID:        nodeID,
		address:       address,
		dbPath:        dbPath,
		storage:       storage,
		testing:       testing,
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

func (n *Node) SetControlPlaneClient(client pb.ControlPlaneClient) {
	n.cpClientMu.Lock()
	defer n.cpClientMu.Unlock()
	n.cpClient = client
}

func (n *Node) SetRole(role NodeRole, nextNode, prevNode string) {
	n.topologyMu.Lock()
	defer n.topologyMu.Unlock()

	//changedNextNode := n.nextNode != nextNode
	changedPrevNode := n.prevNode != prevNode
	if n.role == role && n.nextNode == nextNode && n.prevNode == prevNode {
		return
	}

	n.role = role
	n.nextNode = nextNode
	n.prevNode = prevNode
	log.Printf("Node %s role set to %v, next node: %s, prev node: %s", n.nodeID, n.role, n.nextNode, n.prevNode)

	lastSeq := n.storage.GetLastSequence()
	lastAck, _ := n.storage.GetLastAcked()
	if n.role == RoleTail {
		// The old tail may have not sent us all ACKs yet, so generate the latest ACK here.
		log.Printf("Role changed to tail, sending %d as last ACK", lastSeq)
		if err := n.storage.UpdateLastAcked(lastSeq); err != nil {
			log.Printf("Failed to update last acked: %v", err)
		}
		n.ackQueue <- lastSeq
	}

	if changedPrevNode && n.prevNode != "" {
		n.ackQueue <- lastAck

		// Stream operation from lastSeq+1
		conn, err := grpc.NewClient(n.prevNode, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			// TODO: handle error
			return
		}
		defer conn.Close()
		prevNodeClient := pb.NewReplicationClient(conn)
		stream, err := prevNodeClient.StreamLog(context.Background(), &pb.StreamLogRequest{FromSequence: lastSeq + 1})
		if err != nil {
			// TODO: handle error
			return
		}

		for {
			op, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				// TODO: handle error
				// stream error
				return
			}
			// Apply op
			log.Printf("Applying %d, %s", op.Sequence, op.Operation)
			_, err = n.applyWriteOp(op)
			if err != nil {
				//return fmt.Errorf("failed to apply op %d: %v", op.Sequence, err)
				// TODO: handle error
				// No return here, because this might happen if we applied a duplicate
			}
		}
	}
}

func (n *Node) checkSyncing() error {
	n.syncMu.RLock()
	defer n.syncMu.RUnlock()
	if n.syncing {
		return fmt.Errorf("node is syncing")
	}
	return nil
}

func (n *Node) NotifyStateChange(_ context.Context, req *pb.StateChangeNotification) (*emptypb.Empty, error) {
	log.Printf("Node %s received state change: prevNode=%s, nextNode=%s, isHead=%v, isTail=%v", n.nodeID, req.PrevNode, req.NextNode, req.IsHead, req.IsTail)

	prevNode := req.PrevNode
	nextNode := req.NextNode
	var role NodeRole

	if req.IsHead {
		role = RoleHead
	} else if req.IsTail {
		role = RoleTail
	} else {
		role = RoleMiddle
	}

	n.SetRole(role, nextNode, prevNode)

	return &emptypb.Empty{}, nil
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

	topic, err := n.storage.CreateTopic(req.Name, req.UserId)
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
		LikerId:        req.UserId,
	}

	n.broadcastEvent(event)

	if n.nextNode != "" {
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

func (n *Node) JoinTopic(ctx context.Context, req *pb.JoinTopicRequest) (*emptypb.Empty, error) {
	if err := n.checkSyncing(); err != nil {
		return nil, err
	}
	if n.role != RoleHead {
		return nil, fmt.Errorf("not the head node")
	}

	err := n.storage.AddTopicParticipant(req.TopicId, req.UserId)
	if err != nil {
		return nil, err
	}

	if n.nextNode != "" {
		seq, err := n.storage.LogOperation(&pb.WriteOp{
			Operation: &pb.WriteOp_JoinTopic{JoinTopic: req},
		})
		if err != nil {
			return nil, err
		}

		errChan := make(chan error, 1)
		n.pendingMu.Lock()
		n.pendingWrites[seq] = errChan
		n.pendingMu.Unlock()

		err = n.replicateWrite(ctx, &pb.WriteOp{
			Operation: &pb.WriteOp_JoinTopic{JoinTopic: req},
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

	return &emptypb.Empty{}, nil
}

func (n *Node) LeaveTopic(ctx context.Context, req *pb.LeaveTopicRequest) (*emptypb.Empty, error) {
	if err := n.checkSyncing(); err != nil {
		return nil, err
	}
	if n.role != RoleHead {
		return nil, fmt.Errorf("not the head node")
	}

	err := n.storage.RemoveTopicParticipant(req.TopicId, req.UserId)
	if err != nil {
		return nil, err
	}

	if n.nextNode != "" {
		seq, err := n.storage.LogOperation(&pb.WriteOp{
			Operation: &pb.WriteOp_LeaveTopic{LeaveTopic: req},
		})
		if err != nil {
			return nil, err
		}

		errChan := make(chan error, 1)
		n.pendingMu.Lock()
		n.pendingWrites[seq] = errChan
		n.pendingMu.Unlock()

		err = n.replicateWrite(ctx, &pb.WriteOp{
			Operation: &pb.WriteOp_LeaveTopic{LeaveTopic: req},
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

	return &emptypb.Empty{}, nil
}

// Read operations
func (n *Node) ListTopics(ctx context.Context, req *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
	if err := n.checkSyncing(); err != nil {
		return nil, err
	}
	// Check if we have unacknowledged writes. If so, forward to tail.
	if n.role != RoleTail && n.hasPendingWrites() {
		return n.forwardListTopicsToTail(ctx, req)
	}

	topics, err := n.storage.ListTopics(req.UserId)
	if err != nil {
		return nil, err
	}

	return &pb.ListTopicsResponse{Topics: topics}, nil
}

func (n *Node) ListAllTopics(ctx context.Context, _ *emptypb.Empty) (*pb.ListTopicsResponse, error) {
	if err := n.checkSyncing(); err != nil {
		return nil, err
	}
	// Check if we have unacknowledged writes. If so, forward to tail.
	if n.role != RoleTail && n.hasPendingWrites() {
		return n.forwardListAllTopicsToTail(ctx)
	}

	topics, err := n.storage.ListAllTopics()
	if err != nil {
		return nil, err
	}

	return &pb.ListTopicsResponse{Topics: topics}, nil
}

func (n *Node) ListJoinableTopics(ctx context.Context, req *pb.ListJoinableTopicsRequest) (*pb.ListTopicsResponse, error) {
	if err := n.checkSyncing(); err != nil {
		return nil, err
	}
	// Check if we have unacknowledged writes. If so, forward to tail.
	if n.role != RoleTail && n.hasPendingWrites() {
		return n.forwardListJoinableTopicsToTail(ctx, req)
	}

	topics, err := n.storage.ListJoinableTopics(req.UserId)
	if err != nil {
		return nil, err
	}

	// Filter out DM topics (starting with _)
	filteredTopics := make([]*pb.Topic, 0, len(topics))
	for _, t := range topics {
		if !strings.HasPrefix(t.Name, "_") {
			filteredTopics = append(filteredTopics, t)
		}
	}

	return &pb.ListTopicsResponse{Topics: filteredTopics}, nil
}

func (n *Node) GetMessages(ctx context.Context, req *pb.GetMessagesRequest) (*pb.GetMessagesResponse, error) {
	if err := n.checkSyncing(); err != nil {
		return nil, err
	}
	// Check if we have unacknowledged writes. If so, forward to tail.
	if n.role != RoleTail && n.hasPendingWrites() {
		return n.forwardGetMessagesToTail(ctx, req)
	}

	messages, err := n.storage.GetMessages(req.TopicId, req.FromMessageId, req.Limit, req.RequestUserId)
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

	messages, err := n.storage.GetMessagesByUser(req.TopicId, req.UserName, req.Limit, req.RequestUserId)
	fmt.Println(messages)
	if err != nil {
		return nil, err
	}

	return &pb.GetMessagesResponse{Messages: messages}, nil
}

// Subscription handling
func (n *Node) GetSubscriptionNode(ctx context.Context, _ *pb.SubscriptionNodeRequest) (*pb.SubscriptionNodeResponse, error) {
	if err := n.checkSyncing(); err != nil {
		return nil, err
	}
	if n.role != RoleHead {
		return nil, fmt.Errorf("not the head node")
	}

	n.cpClientMu.RLock()
	cpClient := n.cpClient
	n.cpClientMu.RUnlock()

	if cpClient == nil {
		return nil, fmt.Errorf("control plane client not set")
	}

	chainState, err := cpClient.GetChainState(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, fmt.Errorf("failed to get chain state: %v", err)
	}

	chain := chainState.Chain
	if len(chain) == 0 {
		return nil, fmt.Errorf("chain is empty")
	}

	// Simple round-robin load balancing
	idx := atomic.AddUint64(&n.rrIndex, 1)
	selectedNode := chain[idx%uint64(len(chain))]

	token := generateToken()

	return &pb.SubscriptionNodeResponse{
		SubscribeToken: token,
		Node:           selectedNode,
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
		log.Printf("Subscribing on topic id %d", topicID)
		messages, err := n.storage.GetMessages(topicID, sub.FromMessageID, 1000, req.UserId)
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
func (n *Node) ReplicateWrite(_ context.Context, req *pb.ReplicationRequest) (*emptypb.Empty, error) {
	op := req.Op

	// If testing is enabled and this is node4, check if we should freeze
	if n.testing && n.nodeID == "node4" {
		if op.Operation != nil {
			if postMsg, ok := op.Operation.(*pb.WriteOp_PostMessage); ok {
				if postMsg.PostMessage.Text == "node4freeze" {
					log.Printf("Node %s freezing on message 'node4freeze'", n.nodeID)
					// Freeze indefinitely (or until killed)
					select {}
				}
			}
		}
	}

	event, err := n.applyWriteOp(op)
	if err != nil {
		return nil, err
	}

	if event != nil {
		n.broadcastEvent(event)
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

func (n *Node) applyWriteOp(op *pb.WriteOp) (*pb.MessageEvent, error) {
	// Log the operation first (idempotent)
	if _, err := n.storage.LogOperation(op); err != nil {
		return nil, err
	}

	// TODO: if the operation is a duplicate, return nil, nil here

	switch o := op.Operation.(type) {
	case *pb.WriteOp_CreateUser:
		_, err := n.storage.CreateUser(o.CreateUser.Name, o.CreateUser.Password, o.CreateUser.Salt)
		if err != nil {
			// If user already exists, it might be due to replay. Check if it exists.
			if _, errGet := n.storage.GetUserByName(o.CreateUser.Name); errGet == nil {
				return nil, nil
			}
			return nil, err
		}
	case *pb.WriteOp_CreateTopic:
		_, err := n.storage.CreateTopic(o.CreateTopic.Name, o.CreateTopic.UserId)
		if err != nil {
			if _, errGet := n.storage.GetTopicByName(o.CreateTopic.Name); errGet == nil {
				return nil, nil
			}
			return nil, err
		}
	case *pb.WriteOp_PostMessage:
		msg, err := n.storage.PostMessage(o.PostMessage.TopicId, o.PostMessage.UserId, o.PostMessage.Text)
		if err != nil {
			return nil, err
		}
		return &pb.MessageEvent{
			SequenceNumber: op.Sequence,
			Op:             pb.OpType_OP_POST,
			Message:        msg,
			EventAt:        msg.CreatedAt,
		}, nil
	case *pb.WriteOp_LikeMessage:
		msg, err := n.storage.LikeMessage(o.LikeMessage.TopicId, o.LikeMessage.MessageId, o.LikeMessage.UserId)
		if err != nil {
			return nil, err
		}
		return &pb.MessageEvent{
			SequenceNumber: op.Sequence,
			Op:             pb.OpType_OP_LIKE,
			Message:        msg,
			EventAt:        timestamppb.Now(),
			LikerId:        o.LikeMessage.UserId,
		}, nil
	case *pb.WriteOp_JoinTopic:
		err := n.storage.AddTopicParticipant(o.JoinTopic.TopicId, o.JoinTopic.UserId)
		if err != nil {
			return nil, err
		}
	case *pb.WriteOp_LeaveTopic:
		err := n.storage.RemoveTopicParticipant(o.LeaveTopic.TopicId, o.LeaveTopic.UserId)
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func (n *Node) AcknowledgeWrite(_ context.Context, req *pb.AcknowledgeRequest) (*emptypb.Empty, error) {
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
			// Make this node send a successful response to the client
			ch <- nil
		}
	} else {
		// Propagate acknowledgement to previous node
		n.ackQueue <- req.Sequence
	}
	return &emptypb.Empty{}, nil
}

func (n *Node) GetLastAcked(_ context.Context, _ *emptypb.Empty) (*pb.GetLastAckedResponse, error) {
	seq, err := n.storage.GetLastAcked()
	if err != nil {
		return nil, err
	}
	return &pb.GetLastAckedResponse{Sequence: seq}, nil
}

func (n *Node) GetLastSequence(_ context.Context, _ *emptypb.Empty) (*pb.GetLastSequenceResponse, error) {
	seq := n.storage.GetLastSequence()
	return &pb.GetLastSequenceResponse{Sequence: seq}, nil
}

func (n *Node) runReplicationWorker() {
	for op := range n.replQueue {
		if err := n.replicateWrite(context.Background(), op); err != nil {
			log.Printf("Hard failed to replicate write (seq %d): %v", op.Sequence, err)
		}
	}
}

func (n *Node) runAckWorker() {
	for seq := range n.ackQueue {
		if err := n.acknowledgeWrite(context.Background(), seq); err != nil {
			log.Printf("Hard failed to acknowledge write %d: %v", seq, err)
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

		log.Printf("Sending replicate write (seq %d) from %s to %s", op.Sequence, n.nodeID, next)

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

		log.Printf("Sending ACK %d from %s to %s", sequence, n.nodeID, prev)

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

func (n *Node) GetSnapshot(_ *emptypb.Empty, stream pb.Replication_GetSnapshotServer) error {
	tempSnapshot := fmt.Sprintf("%s.snap.%d", n.dbPath, time.Now().UnixNano())
	defer os.Remove(tempSnapshot)

	// Create a consistent snapshot using VACUUM INTO
	_, err := n.storage.db.Exec("VACUUM INTO ?", tempSnapshot)
	if err != nil {
		return fmt.Errorf("failed to create snapshot: %v", err)
	}

	file, err := os.Open(tempSnapshot)
	if err != nil {
		return fmt.Errorf("failed to open snapshot file: %v", err)
	}
	defer file.Close()

	buffer := make([]byte, 64*1024) // 64KB chunks
	for {
		n, err := file.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read snapshot file: %v", err)
		}

		if err := stream.Send(&pb.SnapshotChunk{Content: buffer[:n]}); err != nil {
			return fmt.Errorf("failed to send snapshot chunk: %v", err)
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

	n.cpClientMu.RLock()
	cpClient := n.cpClient
	n.cpClientMu.RUnlock()

	if cpClient == nil {
		return fmt.Errorf("control plane client not set")
	}

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

	lastSeq := n.storage.GetLastSequence()
	lastAcked, _ := n.storage.GetLastAcked()
	if lastSeq != lastAcked {
		log.Printf("Unacked writes detected (lastSeq: %d, lastAcked: %d). Requesting full snapshot from tail.", lastSeq, lastAcked)

		stream, err := client.GetSnapshot(ctx, &emptypb.Empty{})
		if err != nil {
			return fmt.Errorf("failed to start snapshot stream: %v", err)
		}

		tempDB := n.dbPath + ".sync.tmp"
		// Ensure temp file doesn't exist
		os.Remove(tempDB)

		f, err := os.Create(tempDB)
		if err != nil {
			return fmt.Errorf("failed to create temp db file: %v", err)
		}

		for {
			chunk, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				f.Close()
				os.Remove(tempDB)
				return fmt.Errorf("snapshot stream error: %v", err)
			}
			if _, err := f.Write(chunk.Content); err != nil {
				f.Close()
				os.Remove(tempDB)
				return fmt.Errorf("failed to write to temp db file: %v", err)
			}
		}
		f.Close()

		n.storage.Close()
		if err := os.Rename(tempDB, n.dbPath); err != nil {
			return fmt.Errorf("failed to replace db file: %v", err)
		}

		newStorage, err := NewStorage(n.dbPath)
		if err != nil {
			return fmt.Errorf("failed to create new storage: %v", err)
		}
		n.storage = newStorage
	}

	// Retry only happens when there is a sequence number mismatch. Other failures fail immediately.
	const maxRetries = 5
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			log.Printf("Retry attempt %d/%d after sequence mismatch", attempt, maxRetries)
		}

		// Get local last sequence
		lastSeq := n.storage.GetLastSequence()
		log.Printf("Local last sequence: %d", lastSeq)

		// Stream Log
		stream, err := client.StreamLog(ctx, &pb.StreamLogRequest{FromSequence: lastSeq + 1})
		if err != nil {
			return fmt.Errorf("failed to start log stream: %v", err)
		}

		if n.testing && n.nodeID == "node4" && attempt == 0 {
			log.Printf("Node %s is artificially waiting", n.nodeID)
			time.Sleep(2 * time.Second)
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
			_, err = n.applyWriteOp(op)
			if err != nil {
				return fmt.Errorf("failed to apply op %d: %v", op.Sequence, err)
			}

			// Update last acked sequence since we received it from tail
			if err := n.storage.UpdateLastAcked(op.Sequence); err != nil {
				log.Printf("Failed to update last acked during sync: %v", err)
			}
		}

		log.Printf("Sync completed, verifying sequence numbers...")

		tailLastSeqResponse, err := client.GetLastSequence(ctx, &emptypb.Empty{})

		if err != nil {
			return fmt.Errorf("failed to get tail state: %v", err)
		}

		tailLastSeq := tailLastSeqResponse.Sequence
		localLastSeq := n.storage.GetLastSequence()
		if localLastSeq != tailLastSeq {
			if attempt == maxRetries-1 {
				return fmt.Errorf("sequence number mismatch: local sequence %d != tail sequence %d", localLastSeq, tailLastSeq)
			}

			log.Printf("Sequence number mismatch: local sequence %d != tail sequence %d", localLastSeq, tailLastSeq)
			continue
		}

		log.Printf("Sequence numbers match, confirming sync...")
		break
	}

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
	lastSeq := n.storage.GetLastSequence()
	lastAck, _ := n.storage.GetLastAcked()
	return lastSeq > lastAck
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

func (n *Node) forwardListTopicsToTail(ctx context.Context, req *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
	client, conn, err := n.getTailClient()
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return client.ListTopics(ctx, req)
}

func (n *Node) forwardListAllTopicsToTail(ctx context.Context) (*pb.ListTopicsResponse, error) {
	client, conn, err := n.getTailClient()
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return client.ListAllTopics(ctx, &emptypb.Empty{})
}

func (n *Node) forwardListJoinableTopicsToTail(ctx context.Context, req *pb.ListJoinableTopicsRequest) (*pb.ListTopicsResponse, error) {
	client, conn, err := n.getTailClient()
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return client.ListJoinableTopics(ctx, req)
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
	n.cpClientMu.RLock()
	cpClient := n.cpClient
	n.cpClientMu.RUnlock()

	if cpClient == nil {
		return nil, nil, fmt.Errorf("control plane client not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	state, err := cpClient.GetClusterState(ctx, &emptypb.Empty{})
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
