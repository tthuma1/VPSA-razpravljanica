package client

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	pb "messageboard/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Client struct {
	ControlAddrs []string

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
	c.ControlAddrs = strings.Split(controlAddr, ",")
	return c.RefreshTopology()
}

func (c *Client) RefreshTopology() error {
	var chainState *pb.ChainStateResponse
	var err error

	// Try to find the leader among the known control plane addresses
	for _, addr := range c.ControlAddrs {
		chainState, err = c.getChainStateFromNode(addr)
		if err == nil {
			break
		}
		// Check if error contains leader redirect
		if leaderAddr := parseLeaderRedirect(err); leaderAddr != "" {
			fmt.Printf("Redirecting to leader at %s\n", leaderAddr)
			chainState, err = c.getChainStateFromNode(leaderAddr)
			if err == nil {
				break
			}
		}
	}

	if chainState == nil {
		return fmt.Errorf("failed to get chain state from any control plane node: %v", err)
	}

	if len(chainState.Chain) == 0 {
		return fmt.Errorf("chain is empty")
	}

	// Close existing connections
	c.Close()

	// Connect to head
	c.headConn, err = grpc.NewClient(chainState.Chain[0].Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to head: %w", err)
	}
	c.headClient = pb.NewMessageBoardClient(c.headConn)

	// Connect to all nodes for reading
	c.readConns = make([]*grpc.ClientConn, 0, len(chainState.Chain))
	c.readClients = make([]pb.MessageBoardClient, 0, len(chainState.Chain))

	for _, node := range chainState.Chain {
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

func (c *Client) getChainStateFromNode(addr string) (*pb.ChainStateResponse, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := pb.NewControlPlaneClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	return client.GetChainState(ctx, &emptypb.Empty{})
}

func parseLeaderRedirect(err error) string {
	// Error format: "rpc error: code = Unknown desc = not the leader, current leader is at <addr>"
	re := regexp.MustCompile(`current leader is at ([\w.:]+)`)
	matches := re.FindStringSubmatch(err.Error())
	if len(matches) > 1 {
		return matches[1]
	}
	return ""
}

func (c *Client) getReadClient() pb.MessageBoardClient {
	// round-robin for reading. Forwards reads to write if there are unacked operation at reaedClient.
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

func (c *Client) JoinTopicForUser(ctx context.Context, topicID int64, userID int64) error {
	return c.withRetry(func() error {
		_, err := c.headClient.JoinTopic(ctx, &pb.JoinTopicRequest{TopicId: topicID, UserId: userID})
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

func (c *Client) GetTopicByName(ctx context.Context, name string) (*pb.Topic, error) {
	return c.getReadClient().GetTopic(ctx, &pb.GetTopicRequest{Name: name})
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

func (c *Client) GetUserByName(ctx context.Context, name string) (*pb.User, error) {
	return c.getReadClient().GetUser(ctx, &pb.GetUserRequest{Name: name})
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
