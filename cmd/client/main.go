// cmd/client/main.go
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"regexp"
	"time"

	pb "messageboard/proto"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
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

	app          *tview.Application
	pages        *tview.Pages
	loginForm    *tview.Form
	registerForm *tview.Form
	mainFlex     *tview.Flex
	topicList    *tview.List
	messageView  *tview.TextView
	inputField   *tview.InputField
	statusLine   *tview.TextView

	currentTopic string
	cancelSub    context.CancelFunc

	// Messages for the current topic
	messages []*pb.Message

	// Map message ID to current like count
	messageLikes map[int64]int
	// Map message ID to whether current user liked it
	messageLiked map[int64]bool
}

func main() {
	flag.Parse()

	client := &Client{
		app:          tview.NewApplication(),
		pages:        tview.NewPages(),
		messageLikes: make(map[int64]int),
		messageLiked: make(map[int64]bool),
		messages:     make([]*pb.Message, 0),
	}
	defer client.Close()

	if err := client.Connect(*controlAddr); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}

	client.setupUI()

	if err := client.app.SetRoot(client.pages, true).EnableMouse(true).Run(); err != nil {
		panic(err)
	}
}

func (c *Client) setupUI() {
	// Login Form
	c.loginForm = tview.NewForm().
		AddInputField("Username", "", 20, nil, nil).
		AddPasswordField("Password", "", 20, '*', nil).
		AddButton("Login", func() {
			username := c.loginForm.GetFormItem(0).(*tview.InputField).GetText()
			password := c.loginForm.GetFormItem(1).(*tview.InputField).GetText()
			c.LoginUser(username, password)
		}).
		AddButton("Go to Register", func() {
			c.pages.SwitchToPage("register")
		}).
		AddButton("Quit", func() {
			c.app.Stop()
		})
	c.loginForm.SetBorder(true).SetTitle("Login").SetTitleAlign(tview.AlignCenter)

	// Register Form
	c.registerForm = tview.NewForm().
		AddInputField("Username", "", 20, nil, nil).
		AddPasswordField("Password", "", 20, '*', nil).
		AddButton("Register", func() {
			username := c.registerForm.GetFormItem(0).(*tview.InputField).GetText()
			password := c.registerForm.GetFormItem(1).(*tview.InputField).GetText()
			c.RegisterUser(username, password)
		}).
		AddButton("Back to Login", func() {
			c.pages.SwitchToPage("login")
		}).
		AddButton("Quit", func() {
			c.app.Stop()
		})
	c.registerForm.SetBorder(true).SetTitle("Register").SetTitleAlign(tview.AlignCenter)

	// Main UI
	c.topicList = tview.NewList().ShowSecondaryText(false).SetHighlightFullLine(true)
	c.topicList.SetBorder(true).SetTitle("Topics")
	c.topicList.SetSelectedFunc(func(index int, mainText string, secondaryText string, shortcut rune) {
		c.selectTopic(mainText)

		c.topicList.
			SetSelectedBackgroundColor(tcell.ColorBlue).
			SetSelectedTextColor(tcell.ColorWhite)

	}).SetHighlightFullLine(false)

	c.messageView = tview.NewTextView().
		SetDynamicColors(true).
		SetRegions(true).
		SetWordWrap(true).
		SetChangedFunc(func() {
			c.app.Draw()
		})
	c.messageView.SetBorder(true).SetTitle("Messages")

	// Handle clicks on regions (like buttons)
	c.messageView.SetMouseCapture(func(action tview.MouseAction, event *tcell.EventMouse) (tview.MouseAction, *tcell.EventMouse) {
		if action == tview.MouseLeftClick {
			// We need to wait for the click to be processed by TextView so it updates highlights
			go func() {
				time.Sleep(50 * time.Millisecond)
				c.app.QueueUpdate(func() {
					highlights := c.messageView.GetHighlights()
					if len(highlights) > 0 {
						regionID := highlights[0]
						c.handleLikeClick(regionID)
						c.messageView.Highlight() // Clear highlights
					}
				})
			}()
		}
		return action, event
	})

	c.inputField = tview.NewInputField().
		SetLabel("Message: ").
		SetFieldWidth(0).
		SetAcceptanceFunc(func(text string, ch rune) bool {
			// Allow typing if topic selected (by default it is not)
			return c.currentTopic != ""
		})
	c.inputField.SetFieldStyle(tcell.StyleDefault.Foreground(tcell.ColorWhite).Background(tcell.ColorBlue))
	c.inputField.SetDoneFunc(func(key tcell.Key) {
		if key == tcell.KeyEnter {
			text := c.inputField.GetText()
			if text != "" {
				c.postMessage(text)
				c.inputField.SetText("")
			}
		}
	})

	c.statusLine = tview.NewTextView().SetDynamicColors(true)
	c.statusLine.SetText("Welcome! Select a topic to start chatting.")

	leftPanel := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(c.topicList, 0, 1, true).
		AddItem(nil, 1, 0, false).
		AddItem(tview.NewButton("Create Topic").SetSelectedFunc(func() {
			c.showCreateTopicForm()
		}), 3, 0, false).
		AddItem(nil, 1, 0, false).
		AddItem(tview.NewButton("Refresh").SetSelectedFunc(func() {
			c.refreshTopics()
			if c.currentTopic != "" {
				c.selectTopic(c.currentTopic)
			}
		}), 3, 0, false).
		AddItem(nil, 1, 0, false).
		AddItem(tview.NewButton("Logout").SetSelectedFunc(func() {
			c.Logout()
		}), 3, 0, false)

	rightPanel := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(c.messageView, 0, 1, false).
		AddItem(c.inputField, 3, 0, false).
		AddItem(c.statusLine, 1, 0, false)

	c.mainFlex = tview.NewFlex().
		AddItem(leftPanel, 20, 0, true).
		AddItem(rightPanel, 0, 1, false)

	// Add pages
	c.pages.AddPage("login", c.center(c.loginForm, 40, 15), true, true)
	c.pages.AddPage("register", c.center(c.registerForm, 40, 15), true, false)
	c.pages.AddPage("main", c.mainFlex, true, false)
}

func (c *Client) handleLikeClick(regionID string) {
	// Region ID format: "like_<msgID>"
	var msgID int64
	_, err := fmt.Sscanf(regionID, "like_%d", &msgID)
	if err != nil {
		return
	}

	c.LikeMessage(msgID)
}

func (c *Client) center(p tview.Primitive, width, height int) tview.Primitive {
	return tview.NewFlex().
		AddItem(nil, 0, 1, false).
		AddItem(tview.NewFlex().SetDirection(tview.FlexRow).
			AddItem(nil, 0, 1, false).
			AddItem(p, height, 1, true).
			AddItem(nil, 0, 1, false), width, 1, true).
		AddItem(nil, 0, 1, false)
}

func (c *Client) showMainUI() {
	c.pages.SwitchToPage("main")
	c.refreshTopics()

	c.currentTopic = ""
	c.topicList.SetCurrentItem(-1)
	c.topicList.SetHighlightFullLine(false)
	c.app.SetFocus(c.topicList)

	// No topic selected by default = no highlight
	c.topicList.
		SetSelectedTextColor(tcell.ColorWhite).
		SetSelectedBackgroundColor(tcell.ColorDefault)
}

func (c *Client) showCreateTopicForm() {
	input := tview.NewInputField().SetLabel("Topic Name").SetFieldWidth(20)
	form := tview.NewForm().
		AddFormItem(input).
		AddButton("Create", func() {
			name := input.GetText()
			if name != "" {
				c.pages.RemovePage("create_topic_form")
				go func() {
					c.CreateTopic(name)
					// Delay for it to reach tail TODO: mybe some improvements?
					time.Sleep(500 * time.Millisecond)
					c.app.QueueUpdateDraw(func() {
						c.refreshTopics()
					})
				}()
			}
		}).
		AddButton("Cancel", func() {
			c.pages.RemovePage("create_topic_form")
		})
	form.SetBorder(true).SetTitle("Create Topic")

	c.pages.AddPage("create_topic_form", c.center(form, 40, 10), true, true)
}

func (c *Client) refreshTopics() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := c.getReadClient().ListTopics(ctx, &emptypb.Empty{})
	if err != nil {
		c.statusLine.SetText(fmt.Sprintf("Error listing topics: %v", err))
		return
	}

	c.topicList.Clear()
	for _, topic := range resp.Topics {
		c.topicList.AddItem(topic.Name, "", 0, nil)
	}

	if c.currentTopic == "" {
		c.topicList.SetCurrentItem(-1)
	}
}

func (c *Client) selectTopic(name string) {
	if c.cancelSub != nil {
		c.cancelSub()
		c.cancelSub = nil
	}

	c.currentTopic = name
	c.messageView.Clear()
	c.messageView.SetTitle(fmt.Sprintf("Messages: %s", name))
	c.statusLine.SetText(fmt.Sprintf("Joined topic: %s", name))
	c.app.SetFocus(c.inputField)

	// Reset message tracking for new topic
	c.messages = make([]*pb.Message, 0)
	c.messageLikes = make(map[int64]int)
	c.messageLiked = make(map[int64]bool)

	// Start subscription
	ctx, cancel := context.WithCancel(context.Background())
	c.cancelSub = cancel
	go c.Subscribe(ctx, []string{name})
}

func (c *Client) postMessage(text string) {
	if c.currentTopic == "" {
		c.statusLine.SetText("Please select a topic first.")
		return
	}
	c.PostMessage(c.currentTopic, text)
}

// grcp client

func (c *Client) Connect(controlAddr string) error {
	c.controlAddr = controlAddr
	return c.RefreshTopology()
}

func (c *Client) RefreshTopology() error {
	conn, err := grpc.NewClient(c.controlAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to control plane: %w", err)
	}

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
	//fmt.Printf("Topology refreshed. Head: %s, Read nodes: %d\n", state.Chain[0].Address, len(c.readClients))
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

func (c *Client) withRetry(op func() error) {
	err := op()
	if err == nil {
		return
	}

	// fmt.Printf("Operation failed: %v. Refreshing topology...\n", err)
	if refreshErr := c.RefreshTopology(); refreshErr != nil {
		// fmt.Printf("Failed to refresh topology: %v\n", refreshErr)
		c.showError(status.Convert(err).Message())
		return
	}

	if err := op(); err != nil {
		// fmt.Printf("Operation failed after retry: %v\n", err)
		c.showError(status.Convert(err).Message())
	}
}

func (c *Client) RegisterUser(name, password string) {
	go func() {
		c.withRetry(func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			user, err := c.headClient.CreateUser(ctx, &pb.CreateUserRequest{Name: name, Password: password})
			if err != nil {
				c.showError(status.Convert(err).Message())
				return err
			}

			c.app.QueueUpdateDraw(func() {
				c.currentUser = user
				c.showMainUI()
			})
			return nil
		})
	}()
}

func (c *Client) LoginUser(name, password string) {
	go func() {
		c.withRetry(func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			user, err := c.getReadClient().Login(ctx, &pb.LoginRequest{Name: name, Password: password})
			if err != nil {
				c.showError(status.Convert(err).Message())
				return err
			}

			c.app.QueueUpdateDraw(func() {
				c.currentUser = user
				c.showMainUI()
			})
			return nil
		})
	}()
}

func (c *Client) Logout() {
	c.currentUser = nil
	if c.cancelSub != nil {
		c.cancelSub()
	}
	c.currentTopic = ""
	c.messageView.Clear()
	c.messageView.SetTitle("Messages")
	c.statusLine.SetText("Welcome! Select a topic to start chatting.")

	// Clear password field
	if passwordField, ok := c.loginForm.GetFormItem(1).(*tview.InputField); ok {
		passwordField.SetText("")
	}

	// Clear register
	if regUserField, ok := c.registerForm.GetFormItem(0).(*tview.InputField); ok {
		regUserField.SetText("")
	}
	if regPassField, ok := c.registerForm.GetFormItem(1).(*tview.InputField); ok {
		regPassField.SetText("")
	}

	c.pages.SwitchToPage("login")
}

func (c *Client) CreateTopic(name string) {
	c.withRetry(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_, err := c.headClient.CreateTopic(ctx, &pb.CreateTopicRequest{Name: name})
		if err != nil {
			c.showError(status.Convert(err).Message())
		}
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
	go func() {
		c.withRetry(func() error {
			topicID, err := c.getTopicID(topicName)
			if err != nil {
				c.app.QueueUpdateDraw(func() {
					c.statusLine.SetText(status.Convert(err).Message())
				})
				return err
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			_, err = c.headClient.PostMessage(ctx, &pb.PostMessageRequest{
				TopicId: topicID,
				UserId:  c.currentUser.Id,
				Text:    text,
			})
			if err != nil {
				c.app.QueueUpdateDraw(func() {
					c.statusLine.SetText(status.Convert(err).Message())
				})
				return err
			}

			return nil
		})
	}()
}

func (c *Client) LikeMessage(messageID int64) {
	go func() {
		c.withRetry(func() error {
			topicID, err := c.getTopicID(c.currentTopic)
			if err != nil {
				return fmt.Errorf("finding topic '%s': %w", c.currentTopic, err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// Optimistic update
			c.app.QueueUpdateDraw(func() {
				liked := c.messageLiked[messageID]
				if liked {
					c.messageLikes[messageID]--
					c.messageLiked[messageID] = false
				} else {
					c.messageLikes[messageID]++
					c.messageLiked[messageID] = true
				}

				// Update only the like region
				currentLikes := c.messageLikes[messageID]
				isLiked := c.messageLiked[messageID]

				likeIcon := "♡"
				likeColor := "white"
				if isLiked {
					likeIcon = "♥"
					likeColor = "red"
				}

				regionID := fmt.Sprintf("like_%d", messageID)
				newContent := fmt.Sprintf("[\"%s\"][%s]%s %d[\"\"]", regionID, likeColor, likeIcon, currentLikes)

				text := c.messageView.GetText(false)
				// Find the like region
				// TODO: this can probably be done better with GetRegionText
				re := regexp.MustCompile(fmt.Sprintf(`\["%s"\](.*?)\[""\]`, regionID))
				newText := re.ReplaceAllString(text, newContent)

				if newText != text {
					c.messageView.SetText(newText)
				}

				c.statusLine.SetText(fmt.Sprintf("Liked/Unliked message %d", messageID))
			})

			_, err = c.headClient.LikeMessage(ctx, &pb.LikeMessageRequest{
				TopicId:   topicID,
				MessageId: messageID,
				UserId:    c.currentUser.Id,
			})
			if err != nil {
				// Revert optimistic update on error
				c.app.QueueUpdateDraw(func() {
					// Revert logic...
					c.statusLine.SetText(fmt.Sprintf("Failed to like message: %v", err))
				})
				return err
			}

			return nil
		})
	}()
}

func (c *Client) Subscribe(ctx context.Context, topicNames []string) {
	var lastMessageID int64 = 0

	for {
		err := c.subscribeInternal(ctx, topicNames, &lastMessageID)
		if err == nil {
			return
		}

		if s, ok := status.FromError(err); ok && s.Code() == codes.Canceled {
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

func (c *Client) subscribeInternal(ctx context.Context, topicNames []string, lastMessageID *int64) error {
	topicIDs := make([]int64, 0)
	for _, name := range topicNames {
		tid, err := c.getTopicID(name)
		if err != nil {
			// fmt.Errorf("resolving topic %s: %w", name, err)
			return err
		}
		topicIDs = append(topicIDs, tid)
	}

	if len(topicIDs) == 0 {
		// fmt.Println("No valid topics to subscribe to.")
		return nil
	}

	subCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	subResp, err := c.headClient.GetSubscriptionNode(subCtx, &pb.SubscriptionNodeRequest{
		UserId:  c.currentUser.Id,
		TopicId: topicIDs,
	})
	cancel()
	if err != nil {
		c.app.QueueUpdateDraw(func() {
			c.statusLine.SetText(status.Convert(err).Message())
		})
		return err
	}

	conn, err := grpc.NewClient(subResp.Node.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		// fmt.Errorf("connecting to subscription node: %w", err)
		return err
	}
	defer conn.Close()

	client := pb.NewMessageBoardClient(conn)

	stream, err := client.SubscribeTopic(ctx, &pb.SubscribeTopicRequest{
		UserId:         c.currentUser.Id,
		TopicId:        topicIDs,
		FromMessageId:  *lastMessageID,
		SubscribeToken: subResp.SubscribeToken,
	})
	if err != nil {
		// fmt.Errorf("subscribing: %w", err)
		return err
	}

	for {
		event, err := stream.Recv()
		if err != nil {
			// fmt.Errorf("stream error: %w", err)
			return err
		}

		c.app.QueueUpdateDraw(func() {
			if event.Op == pb.OpType_OP_POST {
				if event.Message != nil && event.Message.Id > *lastMessageID {
					*lastMessageID = event.Message.Id
				}

				// Check if we already have this message (deduplication)
				exists := false
				for _, m := range c.messages {
					if m.Id == event.Message.Id {
						exists = true
						break
					}
				}

				if !exists {
					c.messages = append(c.messages, event.Message)
					c.messageLikes[event.Message.Id] = int(event.Message.Likes)
					c.messageLiked[event.Message.Id] = event.Message.IsLiked

					// Append to view
					c.printMessage(event.Message)
				}
			} else if event.Op == pb.OpType_OP_LIKE || event.Op == pb.OpType_OP_UNLIKE {
				// Update like count
				var msgID int64 = event.Message.Id
				var currentLikes int32
				var isLiked bool
				found := false

				for _, m := range c.messages {
					if m.Id == msgID {
						m.Likes = event.Message.Likes
						c.messageLikes[m.Id] = int(m.Likes)

						if event.LikerId == c.currentUser.Id {
							c.messageLiked[m.Id] = event.Message.IsLiked
						}
						currentLikes = m.Likes
						isLiked = c.messageLiked[m.Id]
						found = true
						break
					}
				}

				if found {
					likeIcon := "♡"
					likeColor := "white"
					if isLiked {
						likeIcon = "♥"
						likeColor = "red"
					}

					regionID := fmt.Sprintf("like_%d", msgID)
					newContent := fmt.Sprintf("[\"%s\"][%s]%s %d[\"\"]", regionID, likeColor, likeIcon, currentLikes)

					text := c.messageView.GetText(false)
					re := regexp.MustCompile(fmt.Sprintf(`\["%s"\](.*?)\[""\]`, regionID))
					newText := re.ReplaceAllString(text, newContent)

					if newText != text {
						c.messageView.SetText(newText)
					}
				}
			}
		})
	}
}

func (c *Client) printMessage(msg *pb.Message) {
	userName := c.getUserName(msg.UserId)

	// Determine heart icon and color
	likeIcon := "♡"
	likeColor := "white"
	if c.messageLiked[msg.Id] {
		likeIcon = "♥"
		likeColor = "red"
	}

	likes := c.messageLikes[msg.Id]

	// Format: [User]: Message
	//         [LikeButton] Count
	fmt.Fprintf(c.messageView, "[yellow]%s[white]: %s\n", userName, msg.Text)

	// Create a region for the like button
	regionID := fmt.Sprintf("like_%d", msg.Id)
	fmt.Fprintf(c.messageView, "   [\"%s\"][%s]%s %d[\"\"]\n", regionID, likeColor, likeIcon, likes)
}

func (c *Client) getUserName(userID int64) string {
	if c.currentUser != nil && c.currentUser.Id == userID {
		return c.currentUser.Name
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	user, err := c.getReadClient().GetUserById(ctx, &pb.GetUserByIdRequest{Id: userID})
	if err != nil {
		return fmt.Sprintf("User %d", userID)
	}

	return user.Name
}

func (c *Client) showError(msg string) {
	c.app.QueueUpdateDraw(func() {
		modal := tview.NewModal().
			SetText(msg).
			AddButtons([]string{"OK"}).
			SetDoneFunc(func(buttonIndex int, buttonLabel string) {
				c.pages.RemovePage("error")
			})
		c.pages.AddPage("error", modal, true, true)
	})
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
