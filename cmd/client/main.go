// cmd/client/main.go
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	pb "messageboard/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

var (
	controlAddr = flag.String("control", "localhost:50050", "Control plane address")
)

type Client struct {
	controlAddr string

	headConn   *grpc.ClientConn
	headClient pb.MessageBoardClient

	readConns   []*grpc.ClientConn
	readClients []pb.MessageBoardClient
	readIndex   int

	currentUser *pb.User
}

func main() {
	flag.Parse()

	client := &Client{}
	defer client.Close()

	if err := client.Connect(*controlAddr); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}

	fmt.Println("=== MessageBoard Client ===")

	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println("Please login or register:")
	fmt.Println("  1. register <name> <password>")
	fmt.Println("  2. login <name> <password>")
	fmt.Println("  3. quit")
	fmt.Print("> ")

	// Main application loop
	for {
		// Authentication Loop
		for client.currentUser == nil {
			if !scanner.Scan() {
				return
			}

			line := scanner.Text()
			parts := strings.Fields(line)
			if len(parts) == 0 {
				continue
			}

			cmd := parts[0]
			args := parts[1:]

			switch cmd {
			case "register":
				if len(args) < 2 {
					fmt.Println("Usage: register <name> <password>")
					continue
				}
				client.RegisterUser(args[0], args[1])
			case "login":
				if len(args) < 2 {
					fmt.Println("Usage: login <name> <password>")
					continue
				}
				client.LoginUser(args[0], args[1])
			case "quit", "exit":
				return
			default:
				fmt.Println("Unknown command. Please register or login first.")
				fmt.Println("  1. register <name> <password>")
				fmt.Println("  2. login <name> <password>")
				fmt.Println("  3. quit")
				fmt.Print("> ")
			}
		}

		fmt.Printf("\nWelcome, %s (ID: %d)!\n", client.currentUser.Name, client.currentUser.Id)
		fmt.Println("Commands:")
		fmt.Println("  1. create-topic <name>")
		fmt.Println("  2. post <topic_name> <text>")
		fmt.Println("  3. like <topic_name> <message_id>")
		fmt.Println("  4. list-topics")
		fmt.Println("  5. get-messages-after <topic_name> [from_id] [limit]")
		fmt.Println("  6. get-messages-user <topic_name> <user_name> [limit]")
		fmt.Println("  7. subscribe <topic_name1> [topic_name2...]")
		fmt.Println("  8. logout")
		fmt.Println("  9. quit")
		fmt.Println()

		// Command Loop
		logout := false
		for !logout {
			fmt.Print("> ")
			if !scanner.Scan() {
				return
			}

			line := scanner.Text()
			parts := strings.Fields(line)
			if len(parts) == 0 {
				continue
			}

			cmd := parts[0]
			args := parts[1:]

			switch cmd {
			case "create-topic":
				if len(args) < 1 {
					fmt.Println("Usage: create-topic <name>")
					continue
				}
				client.CreateTopic(strings.Join(args, " "))

			case "post":
				if len(args) < 2 {
					fmt.Println("Usage: post <topic_name> <text>")
					continue
				}
				topicName := args[0]
				text := strings.Join(args[1:], " ")
				client.PostMessage(topicName, text)

			case "like":
				if len(args) < 2 {
					fmt.Println("Usage: like <topic_name> <message_id>")
					continue
				}
				topicName := args[0]
				messageID, _ := strconv.ParseInt(args[1], 10, 64)
				client.LikeMessage(topicName, messageID)

			case "list-topics":
				client.ListTopics()

			case "get-messages-after":
				if len(args) < 1 {
					fmt.Println("Usage: get-messages-after <topic_name> [from_id] [limit]")
					continue
				}
				topicName := args[0]
				fromID := int64(0)
				limit := int32(10)
				if len(args) > 1 {
					fromID, _ = strconv.ParseInt(args[1], 10, 64)
				}
				if len(args) > 2 {
					l, _ := strconv.ParseInt(args[2], 10, 32)
					limit = int32(l)
				}
				client.GetMessages(topicName, fromID, limit)

			case "get-messages-user":
				if len(args) < 2 {
					fmt.Println("Usage: get-messages-user <topic_name> <user_name> [limit]")
					continue
				}
				topicName := args[0]
				userName := args[1]
				limit := int32(10)
				if len(args) > 2 {
					l, _ := strconv.ParseInt(args[2], 10, 32)
					limit = int32(l)
				}
				client.GetMessagesByUser(topicName, userName, limit)

			case "subscribe":
				if len(args) < 1 {
					fmt.Println("Usage: subscribe <topic_name1> [topic_name2...]")
					continue
				}
				client.Subscribe(args)

			case "logout":
				client.currentUser = nil
				logout = true
				fmt.Println("Logged out.")

			case "quit", "exit":
				return

			default:
				fmt.Println("Unknown command")
			}
		}
	}
}

func (c *Client) Connect(controlAddr string) error {
	c.controlAddr = controlAddr
	return c.RefreshTopology()
}

func (c *Client) RefreshTopology() error {
	conn, err := grpc.NewClient(c.controlAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to control plane: %w", err)
	}
	defer conn.Close()

	client := pb.NewControlPlaneClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	state, err := client.GetChainState(ctx, &emptypb.Empty{})
	if err != nil {
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
	fmt.Printf("Topology refreshed. Head: %s, Read nodes: %d\n", state.Chain[0].Address, len(c.readClients))
	return nil
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

func (c *Client) getReadClient() pb.MessageBoardClient {
	if len(c.readClients) == 0 {
		return c.headClient
	}
	client := c.readClients[c.readIndex]
	c.readIndex = (c.readIndex + 1) % len(c.readClients)
	return client
}

func (c *Client) withRetry(op func() error) {
	err := op()
	if err == nil {
		return
	}

	fmt.Printf("Operation failed: %v. Refreshing topology...\n", err)
	if refreshErr := c.RefreshTopology(); refreshErr != nil {
		fmt.Printf("Failed to refresh topology: %v\n", refreshErr)
		return
	}

	if err := op(); err != nil {
		fmt.Printf("Operation failed after retry: %v\n", err)
	}
}

func (c *Client) RegisterUser(name, password string) {
	c.withRetry(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		user, err := c.headClient.CreateUser(ctx, &pb.CreateUserRequest{Name: name, Password: password})
		if err != nil {
			return err
		}

		c.currentUser = user
		fmt.Printf("Registered successfully: ID=%d, Name=%s\n", user.Id, user.Name)
		return nil
	})
}

func (c *Client) LoginUser(name, password string) {
	c.withRetry(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		user, err := c.getReadClient().Login(ctx, &pb.LoginRequest{Name: name, Password: password})
		if err != nil {
			return err
		}

		c.currentUser = user
		fmt.Printf("Logged in successfully: ID=%d, Name=%s\n", user.Id, user.Name)
		return nil
	})
}

func (c *Client) CreateTopic(name string) {
	c.withRetry(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		topic, err := c.headClient.CreateTopic(ctx, &pb.CreateTopicRequest{Name: name})
		if err != nil {
			return err
		}

		fmt.Printf("Created topic: ID=%d, Name=%s\n", topic.Id, topic.Name)
		return nil
	})
}

func (c *Client) getTopicID(name string) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	topic, err := c.getReadClient().GetTopic(ctx, &pb.GetTopicRequest{Name: name})
	if err != nil {
		return 0, err
	}
	return topic.Id, nil
}

func (c *Client) PostMessage(topicName string, text string) {
	c.withRetry(func() error {
		topicID, err := c.getTopicID(topicName)
		if err != nil {
			return fmt.Errorf("finding topic '%s': %w", topicName, err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		msg, err := c.headClient.PostMessage(ctx, &pb.PostMessageRequest{
			TopicId: topicID,
			UserId:  c.currentUser.Id,
			Text:    text,
		})
		if err != nil {
			return err
		}

		fmt.Printf("Posted message: ID=%d, User=%d, Likes=%d\n", msg.Id, msg.UserId, msg.Likes)
		return nil
	})
}

func (c *Client) LikeMessage(topicName string, messageID int64) {
	c.withRetry(func() error {
		topicID, err := c.getTopicID(topicName)
		if err != nil {
			return fmt.Errorf("finding topic '%s': %w", topicName, err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		msg, err := c.headClient.LikeMessage(ctx, &pb.LikeMessageRequest{
			TopicId:   topicID,
			MessageId: messageID,
			UserId:    c.currentUser.Id,
		})
		if err != nil {
			return err
		}

		fmt.Printf("Liked message: ID=%d, Likes=%d\n", msg.Id, msg.Likes)
		return nil
	})
}

func (c *Client) ListTopics() {
	c.withRetry(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		resp, err := c.getReadClient().ListTopics(ctx, &emptypb.Empty{})
		if err != nil {
			return err
		}

		fmt.Println("Topics:")
		for _, topic := range resp.Topics {
			fmt.Printf("  [%d] %s\n", topic.Id, topic.Name)
		}
		return nil
	})
}

func (c *Client) GetMessages(topicName string, fromID int64, limit int32) {
	c.withRetry(func() error {
		topicID, err := c.getTopicID(topicName)
		if err != nil {
			return fmt.Errorf("finding topic '%s': %w", topicName, err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		resp, err := c.getReadClient().GetMessages(ctx, &pb.GetMessagesRequest{
			TopicId:       topicID,
			FromMessageId: fromID,
			Limit:         limit,
		})
		if err != nil {
			return err
		}

		fmt.Printf("Messages in topic '%s' (ID: %d):\n", topicName, topicID)
		for _, msg := range resp.Messages {
			fmt.Printf("  [%d] User %d: %s (Likes: %d)\n", msg.Id, msg.UserId, msg.Text, msg.Likes)
		}
		return nil
	})
}

func (c *Client) GetMessagesByUser(topicName string, userName string, limit int32) {
	c.withRetry(func() error {
		topicID, err := c.getTopicID(topicName)
		if err != nil {
			return fmt.Errorf("finding topic '%s': %w", topicName, err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		resp, err := c.getReadClient().GetMessagesByUser(ctx, &pb.GetMessagesByUserRequest{
			TopicId:  topicID,
			UserName: userName,
			Limit:    limit,
		})
		if err != nil {
			return err
		}

		fmt.Printf("Messages in topic '%s' (ID: %d) by user '%s':\n", topicName, topicID, userName)
		for _, msg := range resp.Messages {
			fmt.Printf("  [%d] User %d: %s (Likes: %d)\n", msg.Id, msg.UserId, msg.Text, msg.Likes)
		}
		return nil
	})
}

func (c *Client) Subscribe(topicNames []string) {
	var lastMessageID int64 = 0

	for {
		err := c.subscribeInternal(topicNames, &lastMessageID)
		if err == nil {
			return
		}

		fmt.Printf("Subscription connection lost: %v. Reconnecting...\n", err)
		time.Sleep(1 * time.Second)

		if err := c.RefreshTopology(); err != nil {
			fmt.Printf("Failed to refresh topology: %v. Retrying in 5s...\n", err)
			time.Sleep(5 * time.Second)
		}
	}
}

func (c *Client) subscribeInternal(topicNames []string, lastMessageID *int64) error {
	topicIDs := make([]int64, 0)
	for _, name := range topicNames {
		tid, err := c.getTopicID(name)
		if err != nil {
			return fmt.Errorf("resolving topic %s: %w", name, err)
		}
		topicIDs = append(topicIDs, tid)
	}

	if len(topicIDs) == 0 {
		fmt.Println("No valid topics to subscribe to.")
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Get subscription node
	subResp, err := c.headClient.GetSubscriptionNode(ctx, &pb.SubscriptionNodeRequest{
		UserId:  c.currentUser.Id,
		TopicId: topicIDs,
	})
	if err != nil {
		return fmt.Errorf("getting subscription node: %w", err)
	}

	// Connect to subscription node
	conn, err := grpc.NewClient(subResp.Node.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("connecting to subscription node: %w", err)
	}
	defer conn.Close()

	client := pb.NewMessageBoardClient(conn)

	stream, err := client.SubscribeTopic(context.Background(), &pb.SubscribeTopicRequest{
		UserId:         c.currentUser.Id,
		TopicId:        topicIDs,
		FromMessageId:  *lastMessageID,
		SubscribeToken: subResp.SubscribeToken,
	})
	if err != nil {
		return fmt.Errorf("subscribing: %w", err)
	}

	fmt.Printf("Subscribed to topics %v (IDs: %v). Waiting for events (Ctrl+C to stop)...\n", topicNames, topicIDs)

	for {
		event, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("stream error: %w", err)
		}

		if event.Message != nil && event.Message.Id > *lastMessageID {
			*lastMessageID = event.Message.Id
		}

		opType := "POST"
		if event.Op == pb.OpType_OP_LIKE {
			opType = "LIKE"
		}

		fmt.Printf("[%s] Seq=%d, Msg ID=%d, User=%d, Text=%s, Likes=%d\n",
			opType, event.SequenceNumber, event.Message.Id, event.Message.UserId,
			event.Message.Text, event.Message.Likes)
	}
}
