// cmd/client/main.go
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"regexp"
	"time"

	"messageboard/internal/client"
	pb "messageboard/proto"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	controlAddr = flag.String("control", "localhost:50050", "Control plane address")
)

type UIClient struct {
	*client.Client

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

	c := client.New()
	uiClient := &UIClient{
		Client:       c,
		app:          tview.NewApplication(),
		pages:        tview.NewPages(),
		messageLikes: make(map[int64]int),
		messageLiked: make(map[int64]bool),
		messages:     make([]*pb.Message, 0),
	}
	defer uiClient.Close()

	if err := uiClient.Connect(*controlAddr); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}

	uiClient.setupUI()

	if err := uiClient.app.SetRoot(uiClient.pages, true).EnableMouse(true).Run(); err != nil {
		panic(err)
	}
}

func (c *UIClient) setupUI() {
	// Login Form
	c.loginForm = tview.NewForm().
		AddInputField("Username", "", 20, nil, nil).
		AddPasswordField("Password", "", 20, '*', nil).
		AddButton("Login", func() {
			username := c.loginForm.GetFormItem(0).(*tview.InputField).GetText()
			password := c.loginForm.GetFormItem(1).(*tview.InputField).GetText()
			c.loginUser(username, password)
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
			c.registerUser(username, password)
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
			c.logout()
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

func (c *UIClient) handleLikeClick(regionID string) {
	// Region ID format: "like_<msgID>"
	var msgID int64
	_, err := fmt.Sscanf(regionID, "like_%d", &msgID)
	if err != nil {
		return
	}

	c.likeMessage(msgID)
}

func (c *UIClient) center(p tview.Primitive, width, height int) tview.Primitive {
	return tview.NewFlex().
		AddItem(nil, 0, 1, false).
		AddItem(tview.NewFlex().SetDirection(tview.FlexRow).
			AddItem(nil, 0, 1, false).
			AddItem(p, height, 1, true).
			AddItem(nil, 0, 1, false), width, 1, true).
		AddItem(nil, 0, 1, false)
}

func (c *UIClient) showMainUI() {
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

func (c *UIClient) showCreateTopicForm() {
	input := tview.NewInputField().SetLabel("Topic Name").SetFieldWidth(20)
	form := tview.NewForm().
		AddFormItem(input).
		AddButton("Create", func() {
			name := input.GetText()
			if name != "" {
				c.pages.RemovePage("create_topic_form")
				go func() {
					c.createTopic(name)
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

func (c *UIClient) refreshTopics() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	topics, err := c.ListTopics(ctx)
	if err != nil {
		c.statusLine.SetText(fmt.Sprintf("Error listing topics: %v", err))
		return
	}

	c.topicList.Clear()
	for _, topic := range topics {
		c.topicList.AddItem(topic.Name, "", 0, nil)
	}

	if c.currentTopic == "" {
		c.topicList.SetCurrentItem(-1)
	}
}

func (c *UIClient) selectTopic(name string) {
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
	go c.subscribe(ctx, []string{name})
}

func (c *UIClient) postMessage(text string) {
	if c.currentTopic == "" {
		c.statusLine.SetText("Please select a topic first.")
		return
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err := c.PostMessage(ctx, c.currentTopic, text)
		if err != nil {
			c.app.QueueUpdateDraw(func() {
				c.statusLine.SetText(status.Convert(err).Message())
			})
		}
	}()
}

func (c *UIClient) registerUser(name, password string) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err := c.RegisterUser(ctx, name, password)
		if err != nil {
			c.showError(status.Convert(err).Message())
			return
		}

		c.app.QueueUpdateDraw(func() {
			c.showMainUI()
		})
	}()
}

func (c *UIClient) loginUser(name, password string) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err := c.LoginUser(ctx, name, password)
		if err != nil {
			c.showError(status.Convert(err).Message())
			return
		}

		c.app.QueueUpdateDraw(func() {
			c.showMainUI()
		})
	}()
}

func (c *UIClient) logout() {
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

func (c *UIClient) createTopic(name string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := c.CreateTopic(ctx, name)
	if err != nil {
		c.showError(status.Convert(err).Message())
	}
}

func (c *UIClient) likeMessage(messageID int64) {
	go func() {
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
			re := regexp.MustCompile(fmt.Sprintf(`\["%s"\](.*?)\[""\]`, regionID))
			newText := re.ReplaceAllString(text, newContent)

			if newText != text {
				c.messageView.SetText(newText)
			}

			c.statusLine.SetText(fmt.Sprintf("Liked/Unliked message %d", messageID))
		})

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// We need to pass the current topic name to SetMessageLike
		err := c.SetMessageLike(ctx, c.currentTopic, messageID, c.messageLiked[messageID])
		if err != nil {
			// Revert optimistic update on error
			c.app.QueueUpdateDraw(func() {
				// Revert logic...
				c.statusLine.SetText(fmt.Sprintf("Failed to like message: %v", err))
			})
		}
	}()
}

func (c *UIClient) subscribe(ctx context.Context, topicNames []string) {
	var lastMessageID int64 = 0

	for {
		err := c.subscribeInternal(ctx, topicNames, &lastMessageID)
		if err == nil {
			return
		}

		if s, ok := status.FromError(err); ok && s.Code() == codes.Canceled {
			return
		}

		c.statusLine.SetText(fmt.Sprintf("Subscription connection lost: %v. Reconnecting...\n", err))
		time.Sleep(1 * time.Second)

		if err := c.RefreshTopology(); err != nil {
			c.statusLine.SetText(fmt.Sprintf("Failed to refresh topology: %v. Retrying in 5s...\n", err))
			time.Sleep(5 * time.Second)
		}
	}
}

func (c *UIClient) subscribeInternal(ctx context.Context, topicNames []string, lastMessageID *int64) error {
	allTopics, err := c.ListTopics(ctx)
	if err != nil {
		return err
	}

	topicMap := make(map[string]int64)
	for _, t := range allTopics {
		topicMap[t.Name] = t.Id
	}

	topicIDs := make([]int64, 0)
	for _, name := range topicNames {
		if tid, ok := topicMap[name]; ok {
			topicIDs = append(topicIDs, tid)
		}
	}

	if len(topicIDs) == 0 {
		return nil
	}

	subCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	subResp, err := c.GetSubscriptionNode(subCtx, topicIDs)
	cancel()
	if err != nil {
		c.app.QueueUpdateDraw(func() {
			c.statusLine.SetText(status.Convert(err).Message())
		})
		return err
	}

	stream, err := c.SubscribeTopic(ctx, subResp.Node.Address, topicIDs, *lastMessageID, subResp.SubscribeToken)
	if err != nil {
		return err
	}

	for {
		event, err := stream.Recv()
		if err != nil {
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

						if event.LikerId == c.Client.GetCurrentUserId() {
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

func (c *UIClient) printMessage(msg *pb.Message) {
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

func (c *UIClient) getUserName(userID int64) string {
	if c.Client.IsCurrentUser(userID) {
		return c.Client.GetCurrentUserName()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	user, err := c.GetUserById(ctx, userID)
	if err != nil {
		return fmt.Sprintf("User %d", userID)
	}

	return user.Name
}

func (c *UIClient) showError(msg string) {
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
