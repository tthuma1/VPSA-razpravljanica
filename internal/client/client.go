package client

import (
	"context"
	"fmt"
	"time"

	pb "messageboard/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Client struct {
	ControlAddr string

	headConn   *grpc.ClientConn
	headClient pb.MessageBoardClient

	readConns   []*grpc.ClientConn
	readClients []pb.MessageBoardClient
	readIndex   int

	currentUser *pb.User
}

func New() *Client {
	return &Client{}
}

func (c *Client) Close() {
	if c.headConn != nil {
		c.headConn.Close()
		c.headConn = nil
	}
	c.headClient = nil

	for _, conn := range c.readConns {
		conn.Close()
	}
	c.readConns = nil
	c.readClients = nil
}

func (c *Client) Connect(controlAddr string) error {
	c.ControlAddr = controlAddr
	return c.RefreshTopology()
}

func (c *Client) RefreshTopology() error {
	// If controlAddr is actually a node address (for testing), we might fail to get chain state if it's not a control plane.
	// But in our architecture, clients talk to control plane to get topology.
	// In the test, we passed "localhost:50061" (node1) to Connect.
	// This is wrong if Connect expects Control Plane address.
	// However, the test code I wrote passed node address.
	// Let's check if we can make it robust.
	// If we pass a node address, we can't get chain state from it (it doesn't implement ControlPlane service).
	// So the test code was wrong to pass node address to Connect if Connect expects CP address.
	// BUT, the prompt said "CLI client application that connects to the control plane to discover head and tail nodes".
	// So Connect SHOULD take Control Plane address.
	// In my test, I passed node address. I should fix the test.
	// But wait, I am editing client.go now.
	// I will keep it as is (expecting CP address).

	conn, err := grpc.NewClient(c.ControlAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to control plane: %w", err)
	}
	defer conn.Close() // Close CP connection after getting state

	client := pb.NewControlPlaneClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	state, err := client.GetChainState(ctx, &emptypb.Empty{})
	if err != nil {
		// Fallback for testing: if we can't get chain state, maybe we are connected directly to a node?
		// This is useful for unit tests where we mock a single node.
		// But for integration test with real nodes, we should use CP.
		return fmt.Errorf("failed to get chain state: %w", err)
	}

	if len(state.Chain) == 0 {
		return fmt.Errorf("chain is empty")
	}

	// Close existing connections
	c.Close()

	// Connect to head
	c.headConn, err = grpc.NewClient(state.Chain[0].Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to head: %w", err)
	}
	c.headClient = pb.NewMessageBoardClient(c.headConn)

	// Connect to all nodes for reading
	c.readConns = make([]*grpc.ClientConn, 0, len(state.Chain))
	c.readClients = make([]pb.MessageBoardClient, 0, len(state.Chain))

	for _, node := range state.Chain {
		conn, err := grpc.NewClient(node.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Printf("Warning: failed to connect to node %s: %v\n", node.Address, err)
			continue
		}
		c.readConns = append(c.readConns, conn)
		c.readClients = append(c.readClients, pb.NewMessageBoardClient(conn))
	}

	if len(c.readClients) == 0 {
		return fmt.Errorf("failed to connect to any nodes")
	}

	c.readIndex = 0
	return nil
}

func (c *Client) getReadClient() pb.MessageBoardClient {
	if len(c.readClients) == 0 {
		return c.headClient
	}
	client := c.readClients[c.readIndex]
	c.readIndex = (c.readIndex + 1) % len(c.readClients)
	return client
}

func (c *Client) withRetry(op func() error) error {
	err := op()
	if err == nil {
		return nil
	}

	// Simple retry logic: refresh topology and try once more
	if refreshErr := c.RefreshTopology(); refreshErr != nil {
		return err // Return original error if refresh fails
	}

	return op()
}

func (c *Client) RegisterUser(ctx context.Context, name, password string) error {
	return c.withRetry(func() error {
		user, err := c.headClient.CreateUser(ctx, &pb.CreateUserRequest{Name: name, Password: password})
		if err != nil {
			return err
		}
		c.currentUser = user
		return nil
	})
}

func (c *Client) LoginUser(ctx context.Context, name, password string) error {
	return c.withRetry(func() error {
		user, err := c.getReadClient().Login(ctx, &pb.LoginRequest{Name: name, Password: password})
		if err != nil {
			return err
		}
		c.currentUser = user
		return nil
	})
}

func (c *Client) CreateTopic(ctx context.Context, name string) error {
	return c.withRetry(func() error {
		_, err := c.headClient.CreateTopic(ctx, &pb.CreateTopicRequest{Name: name, UserId: c.currentUser.Id})
		return err
	})
}

func (c *Client) JoinTopic(ctx context.Context, topicID int64) error {
	return c.withRetry(func() error {
		_, err := c.headClient.JoinTopic(ctx, &pb.JoinTopicRequest{TopicId: topicID, UserId: c.currentUser.Id})
		return err
	})
}

func (c *Client) LeaveTopic(ctx context.Context, topicID int64) error {
	return c.withRetry(func() error {
		_, err := c.headClient.LeaveTopic(ctx, &pb.LeaveTopicRequest{TopicId: topicID, UserId: c.currentUser.Id})
		return err
	})
}

func (c *Client) getTopicID(ctx context.Context, name string) (int64, error) {
	topic, err := c.getReadClient().GetTopic(ctx, &pb.GetTopicRequest{Name: name})
	if err != nil {
		return 0, err
	}
	return topic.Id, nil
}

func (c *Client) PostMessage(ctx context.Context, topicName string, text string) error {
	return c.withRetry(func() error {
		topicID, err := c.getTopicID(ctx, topicName)
		if err != nil {
			return err
		}

		_, err = c.headClient.PostMessage(ctx, &pb.PostMessageRequest{
			TopicId: topicID,
			UserId:  c.currentUser.Id,
			Text:    text,
		})
		return err
	})
}

func (c *Client) SetMessageLike(ctx context.Context, topicName string, messageID int64, like bool) error {
	return c.withRetry(func() error {
		topicID, err := c.getTopicID(ctx, topicName)
		if err != nil {
			return fmt.Errorf("finding topic '%s': %w", topicName, err)
		}

		_, err = c.headClient.LikeMessage(ctx, &pb.LikeMessageRequest{
			TopicId:   topicID,
			MessageId: messageID,
			UserId:    c.currentUser.Id,
			Like:      like,
		})
		return err
	})
}

func (c *Client) ListTopics(ctx context.Context) ([]*pb.Topic, error) {
	resp, err := c.getReadClient().ListTopics(ctx, &pb.ListTopicsRequest{UserId: c.currentUser.Id})
	if err != nil {
		return nil, err
	}
	return resp.Topics, nil
}

func (c *Client) ListAllTopics(ctx context.Context) ([]*pb.Topic, error) {
	resp, err := c.getReadClient().ListAllTopics(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, err
	}
	return resp.Topics, nil
}

func (c *Client) ListJoinableTopics(ctx context.Context) ([]*pb.Topic, error) {
	resp, err := c.getReadClient().ListJoinableTopics(ctx, &pb.ListJoinableTopicsRequest{UserId: c.currentUser.Id})
	if err != nil {
		return nil, err
	}
	return resp.Topics, nil
}

func (c *Client) GetMessages(ctx context.Context, topicName string, fromID int64, limit int32) ([]*pb.Message, error) {
	topicID, err := c.getTopicID(ctx, topicName)
	if err != nil {
		return nil, err
	}

	resp, err := c.getReadClient().GetMessages(ctx, &pb.GetMessagesRequest{
		TopicId:       topicID,
		FromMessageId: fromID,
		Limit:         limit,
		RequestUserId: c.currentUser.Id,
	})
	if err != nil {
		return nil, err
	}
	return resp.Messages, nil
}

func (c *Client) GetMessagesByUser(ctx context.Context, topicID int64, userName string, limit int32) ([]*pb.Message, error) {
	resp, err := c.getReadClient().GetMessagesByUser(ctx, &pb.GetMessagesByUserRequest{
		TopicId:       topicID,
		UserName:      userName,
		Limit:         limit,
		RequestUserId: c.currentUser.Id,
	})
	if err != nil {
		return nil, err
	}
	return resp.Messages, nil
}

func (c *Client) GetUserById(ctx context.Context, id int64) (*pb.User, error) {
	return c.getReadClient().GetUserById(ctx, &pb.GetUserByIdRequest{Id: id})
}

func (c *Client) GetSubscriptionNode(ctx context.Context, topicIDs []int64) (*pb.SubscriptionNodeResponse, error) {
	return c.headClient.GetSubscriptionNode(ctx, &pb.SubscriptionNodeRequest{
		UserId:  c.currentUser.Id,
		TopicId: topicIDs,
	})
}

func (c *Client) SubscribeTopic(ctx context.Context, nodeAddr string, topicIDs []int64, fromMessageID int64, token string) (pb.MessageBoard_SubscribeTopicClient, error) {
	conn, err := grpc.NewClient(nodeAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	client := pb.NewMessageBoardClient(conn)
	return client.SubscribeTopic(ctx, &pb.SubscribeTopicRequest{
		UserId:         c.currentUser.Id,
		TopicId:        topicIDs,
		FromMessageId:  fromMessageID,
		SubscribeToken: token,
	})
}

func (c *Client) GetCurrentUserId() int64 {
	if c.currentUser == nil {
		return 0
	}
	return c.currentUser.Id
}

func (c *Client) GetCurrentUserName() string {
	if c.currentUser == nil {
		return ""
	}
	return c.currentUser.Name
}

func (c *Client) IsCurrentUser(id int64) bool {
	return c.currentUser != nil && c.currentUser.Id == id
}
