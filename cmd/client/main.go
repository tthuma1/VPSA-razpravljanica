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
	headConn    *grpc.ClientConn
	tailConn    *grpc.ClientConn
	headClient  pb.MessageBoardClient
	tailClient  pb.MessageBoardClient
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
	conn, err := grpc.NewClient(controlAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to control plane: %w", err)
	}
	defer conn.Close()

	client := pb.NewControlPlaneClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	state, err := client.GetClusterState(ctx, &emptypb.Empty{})
	if err != nil {
		return fmt.Errorf("failed to get cluster state: %w", err)
	}

	// Connect to head
	c.headConn, err = grpc.NewClient(state.Head.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to head: %w", err)
	}
	c.headClient = pb.NewMessageBoardClient(c.headConn)

	// Connect to tail
	c.tailConn, err = grpc.NewClient(state.Tail.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to tail: %w", err)
	}
	c.tailClient = pb.NewMessageBoardClient(c.tailConn)

	fmt.Printf("Connected to head: %s, tail: %s\n", state.Head.Address, state.Tail.Address)
	return nil
}

func (c *Client) RegisterUser(name, password string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	user, err := c.headClient.CreateUser(ctx, &pb.CreateUserRequest{Name: name, Password: password})
	if err != nil {
		fmt.Printf("Error registering: %v\n", err)
		return
	}

	c.currentUser = user
	fmt.Printf("Registered successfully: ID=%d, Name=%s\n", user.Id, user.Name)
}

func (c *Client) LoginUser(name, password string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	user, err := c.tailClient.Login(ctx, &pb.LoginRequest{Name: name, Password: password})
	if err != nil {
		fmt.Printf("Error logging in: %v\n", err)
		return
	}

	c.currentUser = user
	fmt.Printf("Logged in successfully: ID=%d, Name=%s\n", user.Id, user.Name)
}

func (c *Client) CreateTopic(name string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	topic, err := c.headClient.CreateTopic(ctx, &pb.CreateTopicRequest{Name: name})
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Created topic: ID=%d, Name=%s\n", topic.Id, topic.Name)
}

func (c *Client) getTopicID(name string) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	topic, err := c.tailClient.GetTopic(ctx, &pb.GetTopicRequest{Name: name})
	if err != nil {
		return 0, err
	}
	return topic.Id, nil
}

func (c *Client) PostMessage(topicName string, text string) {
	topicID, err := c.getTopicID(topicName)
	if err != nil {
		fmt.Printf("Error finding topic '%s': %v\n", topicName, err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg, err := c.headClient.PostMessage(ctx, &pb.PostMessageRequest{
		TopicId: topicID,
		UserId:  c.currentUser.Id,
		Text:    text,
	})
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Posted message: ID=%d, User=%d, Likes=%d\n", msg.Id, msg.UserId, msg.Likes)
}

func (c *Client) LikeMessage(topicName string, messageID int64) {
	topicID, err := c.getTopicID(topicName)
	if err != nil {
		fmt.Printf("Error finding topic '%s': %v\n", topicName, err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg, err := c.headClient.LikeMessage(ctx, &pb.LikeMessageRequest{
		TopicId:   topicID,
		MessageId: messageID,
		UserId:    c.currentUser.Id,
	})
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Liked message: ID=%d, Likes=%d\n", msg.Id, msg.Likes)
}

func (c *Client) ListTopics() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := c.tailClient.ListTopics(ctx, &emptypb.Empty{})
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Println("Topics:")
	for _, topic := range resp.Topics {
		fmt.Printf("  [%d] %s\n", topic.Id, topic.Name)
	}
}

func (c *Client) GetMessages(topicName string, fromID int64, limit int32) {
	topicID, err := c.getTopicID(topicName)
	if err != nil {
		fmt.Printf("Error finding topic '%s': %v\n", topicName, err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := c.tailClient.GetMessages(ctx, &pb.GetMessagesRequest{
		TopicId:       topicID,
		FromMessageId: fromID,
		Limit:         limit,
	})
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Messages in topic '%s' (ID: %d):\n", topicName, topicID)
	for _, msg := range resp.Messages {
		fmt.Printf("  [%d] User %d: %s (Likes: %d)\n", msg.Id, msg.UserId, msg.Text, msg.Likes)
	}
}

func (c *Client) GetMessagesByUser(topicName string, userName string, limit int32) {
	topicID, err := c.getTopicID(topicName)
	if err != nil {
		fmt.Printf("Error finding topic '%s': %v\n", topicName, err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := c.tailClient.GetMessagesByUser(ctx, &pb.GetMessagesByUserRequest{
		TopicId:  topicID,
		UserName: userName,
		Limit:    limit,
	})
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Messages in topic '%s' (ID: %d) by user '%s':\n", topicName, topicID, userName)
	for _, msg := range resp.Messages {
		fmt.Printf("  [%d] User %d: %s (Likes: %d)\n", msg.Id, msg.UserId, msg.Text, msg.Likes)
	}
}

func (c *Client) Subscribe(topicNames []string) {
	topicIDs := make([]int64, 0)
	for _, name := range topicNames {
		tid, err := c.getTopicID(name)
		if err != nil {
			fmt.Printf("Warning: Could not find topic '%s', skipping. Error: %v\n", name, err)
			continue
		}
		topicIDs = append(topicIDs, tid)
	}

	if len(topicIDs) == 0 {
		fmt.Println("No valid topics to subscribe to.")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Get subscription node
	subResp, err := c.headClient.GetSubscriptionNode(ctx, &pb.SubscriptionNodeRequest{
		UserId:  c.currentUser.Id,
		TopicId: topicIDs,
	})
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	// Connect to subscription node
	conn, err := grpc.NewClient(subResp.Node.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("Error connecting to subscription node: %v\n", err)
		return
	}
	defer conn.Close()

	client := pb.NewMessageBoardClient(conn)

	stream, err := client.SubscribeTopic(context.Background(), &pb.SubscribeTopicRequest{
		UserId:         c.currentUser.Id,
		TopicId:        topicIDs,
		FromMessageId:  0,
		SubscribeToken: subResp.SubscribeToken,
	})
	if err != nil {
		fmt.Printf("Error subscribing: %v\n", err)
		return
	}

	fmt.Printf("Subscribed to topics %v (IDs: %v). Waiting for events (Ctrl+C to stop)...\n", topicNames, topicIDs)

	for {
		event, err := stream.Recv()
		if err != nil {
			fmt.Printf("Stream error: %v\n", err)
			return
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

func (c *Client) Close() {
	if c.headConn != nil {
		c.headConn.Close()
	}
	if c.tailConn != nil {
		c.tailConn.Close()
	}
}
