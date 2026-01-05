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
	headConn   *grpc.ClientConn
	tailConn   *grpc.ClientConn
	headClient pb.MessageBoardClient
	tailClient pb.MessageBoardClient
}

func main() {
	flag.Parse()

	client := &Client{}
	defer client.Close()

	if err := client.Connect(*controlAddr); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}

	fmt.Println("=== MessageBoard Client ===")
	fmt.Println("Commands:")
	fmt.Println("  1. create-user <name>")
	fmt.Println("  2. create-topic <name>")
	fmt.Println("  3. post <topic_id> <user_id> <text>")
	fmt.Println("  4. like <topic_id> <message_id> <user_id>")
	fmt.Println("  5. list-topics")
	fmt.Println("  6. get-messages <topic_id> [from_id] [limit]")
	fmt.Println("  7. subscribe <user_id> <topic_id1> [topic_id2...]")
	fmt.Println("  8. quit")
	fmt.Println()

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}

		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}

		cmd := parts[0]
		args := parts[1:]

		switch cmd {
		case "create-user":
			if len(args) < 1 {
				fmt.Println("Usage: create-user <name>")
				continue
			}
			client.CreateUser(strings.Join(args, " "))

		case "create-topic":
			if len(args) < 1 {
				fmt.Println("Usage: create-topic <name>")
				continue
			}
			client.CreateTopic(strings.Join(args, " "))

		case "post":
			if len(args) < 3 {
				fmt.Println("Usage: post <topic_id> <user_id> <text>")
				continue
			}
			topicID, _ := strconv.ParseInt(args[0], 10, 64)
			userID, _ := strconv.ParseInt(args[1], 10, 64)
			text := strings.Join(args[2:], " ")
			client.PostMessage(topicID, userID, text)

		case "like":
			if len(args) < 3 {
				fmt.Println("Usage: like <topic_id> <message_id> <user_id>")
				continue
			}
			topicID, _ := strconv.ParseInt(args[0], 10, 64)
			messageID, _ := strconv.ParseInt(args[1], 10, 64)
			userID, _ := strconv.ParseInt(args[2], 10, 64)
			client.LikeMessage(topicID, messageID, userID)

		case "list-topics":
			client.ListTopics()

		case "get-messages":
			if len(args) < 1 {
				fmt.Println("Usage: get-messages <topic_id> [from_id] [limit]")
				continue
			}
			topicID, _ := strconv.ParseInt(args[0], 10, 64)
			fromID := int64(0)
			limit := int32(10)
			if len(args) > 1 {
				fromID, _ = strconv.ParseInt(args[1], 10, 64)
			}
			if len(args) > 2 {
				l, _ := strconv.ParseInt(args[2], 10, 32)
				limit = int32(l)
			}
			client.GetMessages(topicID, fromID, limit)

		case "subscribe":
			if len(args) < 2 {
				fmt.Println("Usage: subscribe <user_id> <topic_id1> [topic_id2...]")
				continue
			}
			userID, _ := strconv.ParseInt(args[0], 10, 64)
			topicIDs := make([]int64, 0)
			for _, arg := range args[1:] {
				tid, _ := strconv.ParseInt(arg, 10, 64)
				topicIDs = append(topicIDs, tid)
			}
			client.Subscribe(userID, topicIDs)

		case "quit", "exit":
			return

		default:
			fmt.Println("Unknown command")
		}
	}
}

func (c *Client) Connect(controlAddr string) error {
	conn, err := grpc.Dial(controlAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
	c.headConn, err = grpc.Dial(state.Head.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to head: %w", err)
	}
	c.headClient = pb.NewMessageBoardClient(c.headConn)

	// Connect to tail
	c.tailConn, err = grpc.Dial(state.Tail.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to tail: %w", err)
	}
	c.tailClient = pb.NewMessageBoardClient(c.tailConn)

	fmt.Printf("Connected to head: %s, tail: %s\n", state.Head.Address, state.Tail.Address)
	return nil
}

func (c *Client) CreateUser(name string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	user, err := c.headClient.CreateUser(ctx, &pb.CreateUserRequest{Name: name})
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Created user: ID=%d, Name=%s\n", user.Id, user.Name)
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

func (c *Client) PostMessage(topicID, userID int64, text string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg, err := c.headClient.PostMessage(ctx, &pb.PostMessageRequest{
		TopicId: topicID,
		UserId:  userID,
		Text:    text,
	})
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Posted message: ID=%d, User=%d, Likes=%d\n", msg.Id, msg.UserId, msg.Likes)
}

func (c *Client) LikeMessage(topicID, messageID, userID int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg, err := c.headClient.LikeMessage(ctx, &pb.LikeMessageRequest{
		TopicId:   topicID,
		MessageId: messageID,
		UserId:    userID,
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

func (c *Client) GetMessages(topicID, fromID int64, limit int32) {
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

	fmt.Printf("Messages in topic %d:\n", topicID)
	for _, msg := range resp.Messages {
		fmt.Printf("  [%d] User %d: %s (Likes: %d)\n", msg.Id, msg.UserId, msg.Text, msg.Likes)
	}
}

func (c *Client) Subscribe(userID int64, topicIDs []int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Get subscription node
	subResp, err := c.headClient.GetSubscriptionNode(ctx, &pb.SubscriptionNodeRequest{
		UserId:  userID,
		TopicId: topicIDs,
	})
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	// Connect to subscription node
	conn, err := grpc.Dial(subResp.Node.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("Error connecting to subscription node: %v\n", err)
		return
	}
	defer conn.Close()

	client := pb.NewMessageBoardClient(conn)

	stream, err := client.SubscribeTopic(context.Background(), &pb.SubscribeTopicRequest{
		UserId:         userID,
		TopicId:        topicIDs,
		FromMessageId:  0,
		SubscribeToken: subResp.SubscribeToken,
	})
	if err != nil {
		fmt.Printf("Error subscribing: %v\n", err)
		return
	}

	fmt.Printf("Subscribed to topics %v. Waiting for events (Ctrl+C to stop)...\n", topicIDs)

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
