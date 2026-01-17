// cmd/client/main.go
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	pb "messageboard/proto"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
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
}

func main() {
	flag.Parse()

	client := &Client{
		app:   tview.NewApplication(),
		pages: tview.NewPages(),
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
		AddItem(tview.NewButton("Create Topic").SetSelectedFunc(func() {
			c.showCreateTopicForm()
		}), 3, 0, false).
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

	resp, err := c.tailClient.ListTopics(ctx, &emptypb.Empty{})
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
	conn, err := grpc.NewClient(controlAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to control plane: %w", err)
	}

	client := pb.NewControlPlaneClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	state, err := client.GetClusterState(ctx, &emptypb.Empty{})
	if err != nil {
		return fmt.Errorf("failed to get cluster state: %w", err)
	}

	c.headConn, err = grpc.NewClient(state.Head.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to head: %w", err)
	}
	c.headClient = pb.NewMessageBoardClient(c.headConn)

	c.tailConn, err = grpc.NewClient(state.Tail.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to tail: %w", err)
	}
	c.tailClient = pb.NewMessageBoardClient(c.tailConn)

	return nil
}

func (c *Client) RegisterUser(name, password string) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		user, err := c.headClient.CreateUser(ctx, &pb.CreateUserRequest{Name: name, Password: password})
		if err != nil {
			c.showError(status.Convert(err).Message())
			return
		}

		c.app.QueueUpdateDraw(func() {
			c.currentUser = user
			c.showMainUI()
		})
	}()
}

func (c *Client) LoginUser(name, password string) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		user, err := c.tailClient.Login(ctx, &pb.LoginRequest{Name: name, Password: password})
		if err != nil {
			c.showError(status.Convert(err).Message())
			return
		}

		c.app.QueueUpdateDraw(func() {
			c.currentUser = user
			c.showMainUI()
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := c.headClient.CreateTopic(ctx, &pb.CreateTopicRequest{Name: name})
	if err != nil {
		c.showError(status.Convert(err).Message())
	}
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
	go func() {
		topicID, err := c.getTopicID(topicName)
		if err != nil {
			c.app.QueueUpdateDraw(func() {
				c.statusLine.SetText(status.Convert(err).Message())
			})
			return
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
		}
	}()
}

func (c *Client) Subscribe(ctx context.Context, topicNames []string) {
	topicIDs := make([]int64, 0)
	for _, name := range topicNames {
		tid, err := c.getTopicID(name)
		if err != nil {
			continue
		}
		topicIDs = append(topicIDs, tid)
	}

	if len(topicIDs) == 0 {
		return
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
		return
	}

	conn, err := grpc.NewClient(subResp.Node.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return
	}
	defer conn.Close()

	client := pb.NewMessageBoardClient(conn)

	stream, err := client.SubscribeTopic(ctx, &pb.SubscribeTopicRequest{
		UserId:         c.currentUser.Id,
		TopicId:        topicIDs,
		FromMessageId:  0,
		SubscribeToken: subResp.SubscribeToken,
	})
	if err != nil {
		return
	}

	for {
		event, err := stream.Recv()
		if err != nil {
			return
		}

		// Fetch username for the message
		userName := c.getUserName(event.Message.UserId)

		c.app.QueueUpdateDraw(func() {
			fmt.Fprintf(c.messageView, "[yellow]%s[white]: %s\n", userName, event.Message.Text)
			c.messageView.ScrollToEnd()
		})
	}
}

func (c *Client) getUserName(userID int64) string {
	if c.currentUser != nil && c.currentUser.Id == userID {
		return c.currentUser.Name
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	user, err := c.tailClient.GetUserById(ctx, &pb.GetUserByIdRequest{Id: userID})
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
	}
	if c.tailConn != nil {
		c.tailConn.Close()
	}
}
