// cmd/client/main.go
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"regexp"
	"sort"
	"strings"
	"time"

	"messageboard/internal/client"
	pb "messageboard/proto"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	controlAddr = flag.String("control", "localhost:50051,localhost:50052,localhost:50053", "Comma-separated list of Control plane addresses")
)

type UIClient struct {
	*client.Client

	app          *tview.Application
	pages        *tview.Pages
	loginForm    *tview.Form
	registerForm *tview.Form
	mainFlex     *tview.Flex
	topicList    *tview.TextView // Changed to TextView for custom regions
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

	// Cached topics for navigation
	cachedTopics []*pb.Topic
	navigating   bool
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
		cachedTopics: make([]*pb.Topic, 0),
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

	// Main UI - Topic List as TextView
	c.topicList = tview.NewTextView().
		SetDynamicColors(true).
		SetRegions(true).
		SetWordWrap(false).
		SetScrollable(true)
	c.topicList.SetBorder(true).SetTitle("Topics")

	// Handle highlighting (clicks)
	c.topicList.SetHighlightedFunc(func(added, removed, remaining []string) {
		if c.navigating {
			return
		}

		if len(added) > 0 {
			idStr := added[0]
			if strings.HasPrefix(idStr, "leave_") {
				// Trashcan clicked
				var tID int64
				fmt.Sscanf(idStr, "leave_%d", &tID)

				// Revert highlight to the topic if possible, or just clear
				// We don't want the trashcan to stay highlighted
				c.navigating = true
				c.topicList.Highlight(removed...)
				c.navigating = false

				c.leaveTopicByID(tID)
			} else if strings.HasPrefix(idStr, "topic_") {
				// Topic clicked
				var tID int64
				fmt.Sscanf(idStr, "topic_%d", &tID)

				topic := c.getTopicByID(tID)
				if topic != nil {
					c.selectTopic(topic.Name)
				}

				// Ensure only this topic is highlighted
				c.navigating = true
				c.topicList.Highlight(idStr)
				c.topicList.ScrollToHighlight()
				c.navigating = false
			}
		} else if len(removed) > 0 {
			// If user clicked the already highlighted topic, it gets removed.
			// We should re-highlight it and treat it as selection.
			idStr := removed[0]
			if strings.HasPrefix(idStr, "topic_") {
				c.navigating = true
				c.topicList.Highlight(idStr)
				c.navigating = false

				var tID int64
				fmt.Sscanf(idStr, "topic_%d", &tID)
				topic := c.getTopicByID(tID)
				if topic != nil {
					c.selectTopic(topic.Name)
				}
			}
		}
	})

	// Handle keyboard navigation
	c.topicList.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyDown {
			c.moveTopicSelection(1)
			return nil
		} else if event.Key() == tcell.KeyUp {
			c.moveTopicSelection(-1)
			return nil
		} else if event.Key() == tcell.KeyEnter {
			// Select currently highlighted topic
			highlights := c.topicList.GetHighlights()
			if len(highlights) > 0 && strings.HasPrefix(highlights[0], "topic_") {
				var tID int64
				fmt.Sscanf(highlights[0], "topic_%d", &tID)
				topic := c.getTopicByID(tID)
				if topic != nil {
					c.selectTopic(topic.Name)
				}
			}
			return nil
		} else if event.Rune() == 'x' {
			c.leaveHighlightedTopic()
			return nil
		}
		return event
	})

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
						if strings.HasPrefix(regionID, "like_") {
							c.handleLikeClick(regionID)
						} else if strings.HasPrefix(regionID, "user_") {
							c.handleUserClick(regionID)
						}
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
		AddItem(tview.NewButton("Join Topic").SetSelectedFunc(func() {
			c.showJoinTopicList()
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

func (c *UIClient) handleUserClick(regionID string) {
	var userID int64
	_, err := fmt.Sscanf(regionID, "user_%d", &userID)
	if err != nil {
		return
	}
	c.showUserProfile(userID)
}

func (c *UIClient) showUserProfile(userID int64) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// 1. Get User Details
		user, err := c.GetUserById(ctx, userID)
		if err != nil {
			c.showError(fmt.Sprintf("Failed to get user profile: %v", err))
			return
		}

		// 2. Get User Messages
		messages, err := c.GetMessagesByUser(ctx, 0, user.Name, 10)
		if err != nil {
			c.showError(fmt.Sprintf("Failed to get user messages: %v", err))
			return
		}

		c.app.QueueUpdateDraw(func() {
			// Use a TextView for the main content, without its own border
			textView := tview.NewTextView().
				SetDynamicColors(true).
				SetWordWrap(true).
				SetScrollable(true)

			// Improved text formatting for clarity
			fmt.Fprintf(textView, "[yellow]Recent Messages (%d):[white]\n", len(messages))
			fmt.Fprintf(textView, "[gray]%s\n", strings.Repeat("─", 50)) // Separator

			if len(messages) == 0 {
				fmt.Fprintf(textView, "\nNo messages found.\n")
			} else {
				for _, msg := range messages {
					fmt.Fprintf(textView, "\n[#66c2ff]Topic: %s[white]\n", msg.TopicName)
					fmt.Fprintf(textView, "  %s\n", msg.Text)
					fmt.Fprintf(textView, "  [green]♥ %d[white]\n", msg.Likes)
				}
			}

			// Use a Flex layout to create a modal-like box with a close button at the bottom.
			// This is more idiomatic for tview than trying to force a GUI-style [X] button.
			modal := tview.NewFlex().SetDirection(tview.FlexRow)

			// Content
			modal.AddItem(textView, 0, 1, false)

			// Message button
			if !c.Client.IsCurrentUser(userID) {
				msgButton := tview.NewButton("Message").SetSelectedFunc(func() {
					c.pages.RemovePage("user_profile")
					c.startDirectMessage(user)
				})
				modal.AddItem(msgButton, 1, 0, false)
				modal.AddItem(nil, 1, 0, false) // Spacer
			}

			// Close button
			button := tview.NewButton("Close").SetSelectedFunc(func() {
				c.pages.RemovePage("user_profile")
			})
			modal.AddItem(button, 1, 0, true)

			// Set a border and title on the whole modal
			modal.SetBorder(true).SetTitle(fmt.Sprintf("Profile: %s", user.Name))

			// Capture Esc key on the modal to also close it
			modal.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
				if event.Key() == tcell.KeyEscape {
					c.pages.RemovePage("user_profile")
					return nil
				}
				return event
			})

			c.pages.AddPage("user_profile", c.center(modal, 60, 20), true, true)
		})
	}()
}

func (c *UIClient) startDirectMessage(otherUser *pb.User) {
	go func() {
		// Construct DM topic name: _user1_user2 (sorted alphabetically)
		currentUser := c.Client.GetCurrentUserName()
		otherUserName := otherUser.Name

		names := []string{currentUser, otherUserName}
		sort.Strings(names)
		dmTopicName := fmt.Sprintf("_%s_%s", names[0], names[1])

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Check if topic exists
		topic, err := c.Client.GetTopicByName(ctx, dmTopicName)
		if err != nil {
			// If not found, create it
			if status.Code(err) == codes.NotFound || strings.Contains(err.Error(), "not found") {
				err = c.Client.CreateTopic(ctx, dmTopicName)
				if err != nil {
					c.showError(fmt.Sprintf("Failed to create DM topic: %v", err))
					return
				}
				// Wait a bit for propagation
				time.Sleep(500 * time.Millisecond)
				// Fetch again to get ID
				topic, err = c.Client.GetTopicByName(ctx, dmTopicName)
				if err != nil {
					c.showError(fmt.Sprintf("Failed to get DM topic after creation: %v", err))
					return
				}
			} else {
				c.showError(fmt.Sprintf("Failed to check DM topic: %v", err))
				return
			}
		}

		// Join the topic for current user
		_ = c.Client.JoinTopic(ctx, topic.Id)

		// Also join the topic for the other user so it appears in their list
		err = c.Client.JoinTopicForUser(ctx, topic.Id, otherUser.Id)
		if err != nil {
			fmt.Printf("Failed to auto-join other user to DM: %v\n", err)
		}

		c.app.QueueUpdateDraw(func() {
			c.refreshTopics()
			// Select the topic
			c.selectTopic(dmTopicName)
		})
	}()
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
	c.app.SetFocus(c.topicList)
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

func (c *UIClient) showJoinTopicList() {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		topics, err := c.ListJoinableTopics(ctx)
		if err != nil {
			c.showError(fmt.Sprintf("Error listing joinable topics: %v", err))
			return
		}

		c.app.QueueUpdateDraw(func() {
			list := tview.NewList().ShowSecondaryText(false)
			list.SetBorder(true).SetTitle("Join Topic")

			for _, topic := range topics {
				// Filter out DM topics from join list
				if strings.HasPrefix(topic.Name, "_") {
					continue
				}

				// Capture topic ID for closure
				tID := topic.Id
				tName := topic.Name
				list.AddItem(tName, "", 0, func() {
					c.pages.RemovePage("join_topic_list")
					go func() {
						c.joinTopic(tID, tName)
						time.Sleep(500 * time.Millisecond)
						c.app.QueueUpdateDraw(func() {
							c.refreshTopics()
						})
					}()
				})
			}

			list.AddItem("Cancel", "", 'c', func() {
				c.pages.RemovePage("join_topic_list")
			})

			c.pages.AddPage("join_topic_list", c.center(list, 40, 20), true, true)
		})
	}()
}

func (c *UIClient) joinTopic(topicID int64, topicName string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := c.JoinTopic(ctx, topicID)
	if err != nil {
		c.showError(status.Convert(err).Message())
	} else {
		c.app.QueueUpdateDraw(func() {
			c.statusLine.SetText(fmt.Sprintf("Joined topic: %s", topicName))
		})
	}
}

func (c *UIClient) leaveHighlightedTopic() {
	highlights := c.topicList.GetHighlights()
	for _, id := range highlights {
		if strings.HasPrefix(id, "topic_") {
			var tID int64
			fmt.Sscanf(id, "topic_%d", &tID)
			c.leaveTopicByID(tID)
			return
		}
	}
}

func (c *UIClient) leaveTopicByID(topicID int64) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err := c.LeaveTopic(ctx, topicID)
		if err != nil {
			c.showError(status.Convert(err).Message())
			return
		}

		c.app.QueueUpdateDraw(func() {
			// Check if we left the current topic
			topic := c.getTopicByID(topicID)
			if topic != nil && topic.Name == c.currentTopic {
				if c.cancelSub != nil {
					c.cancelSub()
					c.cancelSub = nil
				}
				c.currentTopic = ""
				c.messages = nil
				c.messageLikes = make(map[int64]int)
				c.messageLiked = make(map[int64]bool)
				c.messageView.Clear()
				c.messageView.SetTitle("Messages")
				c.statusLine.SetText(fmt.Sprintf("Left topic: %s", topic.Name))
			} else {
				c.statusLine.SetText(fmt.Sprintf("Left topic %d", topicID))
			}
			c.refreshTopics()
		})
	}()
}

func (c *UIClient) refreshTopics() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	topics, err := c.ListTopics(ctx)
	if err != nil {
		c.statusLine.SetText(fmt.Sprintf("Error listing topics: %v", err))
		return
	}

	c.cachedTopics = topics
	c.topicList.Clear()

	// Clear highlights so tview state is reset
	c.navigating = true
	c.topicList.Highlight()
	c.navigating = false

	const lineWidth = 15
	const icon = "x"

	for _, topic := range topics {
		name := topic.Name

		// Handle DM display name
		if strings.HasPrefix(name, "_") {
			// Format: _user1_user2
			parts := strings.Split(name, "_")
			if len(parts) >= 3 {
				// parts[0] is empty because string starts with _
				// parts[1] is user1, parts[2] is user2
				u1 := parts[1]
				u2 := parts[2]
				currentUser := c.Client.GetCurrentUserName()
				if u1 == currentUser {
					name = "_" + u2
				} else {
					name = "_" + u1
				}
			}
		}

		if len(name) > lineWidth {
			name = name[:lineWidth]
		}

		// Format: ["topic_ID"] Name [""]   ["leave_ID"] 🗑 [""]
		fmt.Fprintf(
			c.topicList,
			"[\"topic_%d\"]%-*s[\"\"][\"leave_%d\"] %s[\"\"]\n",
			topic.Id,
			lineWidth,
			name,
			topic.Id,
			icon,
		)
	}

	// Restore selection
	if c.currentTopic != "" {
		for _, t := range topics {
			if t.Name == c.currentTopic {
				c.navigating = true
				c.topicList.Highlight(fmt.Sprintf("topic_%d", t.Id))
				c.topicList.ScrollToHighlight()
				c.navigating = false
				break
			}
		}
	}
}

func (c *UIClient) moveTopicSelection(delta int) {
	if len(c.cachedTopics) == 0 {
		return
	}

	// Find current index
	currentIndex := -1
	highlights := c.topicList.GetHighlights()
	if len(highlights) > 0 {
		for _, h := range highlights {
			if strings.HasPrefix(h, "topic_") {
				var tID int64
				fmt.Sscanf(h, "topic_%d", &tID)
				for i, t := range c.cachedTopics {
					if t.Id == tID {
						currentIndex = i
						break
					}
				}
				break
			}
		}
	}

	newIndex := currentIndex + delta
	if newIndex < 0 {
		newIndex = 0
	}
	if newIndex >= len(c.cachedTopics) {
		newIndex = len(c.cachedTopics) - 1
	}

	if newIndex != currentIndex {
		c.navigating = true
		c.topicList.Highlight(fmt.Sprintf("topic_%d", c.cachedTopics[newIndex].Id))
		c.topicList.ScrollToHighlight()
		c.navigating = false
	}
}

func (c *UIClient) getTopicByID(id int64) *pb.Topic {
	for _, t := range c.cachedTopics {
		if t.Id == id {
			return t
		}
	}
	return nil
}

func (c *UIClient) selectTopic(name string) {
	if c.cancelSub != nil {
		c.cancelSub()
		c.cancelSub = nil
	}

	c.currentTopic = name

	displayName := name
	if strings.HasPrefix(name, "_") {
		parts := strings.Split(name, "_")
		if len(parts) >= 3 {
			u1 := parts[1]
			u2 := parts[2]
			currentUser := c.Client.GetCurrentUserName()
			if u1 == currentUser {
				displayName = "_" + u2
			} else {
				displayName = "_" + u1
			}
		}
	}

	c.messageView.Clear()
	c.messageView.SetTitle(fmt.Sprintf("Messages: %s", displayName))
	c.statusLine.SetText(fmt.Sprintf("Joined topic: %s", displayName))
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

		// If it's a DM, ensure the other user is in the topic
		if strings.HasPrefix(c.currentTopic, "_") {
			parts := strings.Split(c.currentTopic, "_")
			if len(parts) >= 3 {
				u1 := parts[1]
				u2 := parts[2]
				otherUserName := u1
				if u1 == c.Client.GetCurrentUserName() {
					otherUserName = u2
				}

				otherUser, err := c.Client.GetUserByName(ctx, otherUserName)
				if err == nil {
					topic, err := c.Client.GetTopicByName(ctx, c.currentTopic)
					if err == nil {
						_ = c.Client.JoinTopicForUser(ctx, topic.Id, otherUser.Id)
					}
				}
			}
		}

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

	if strings.HasPrefix(name, "_") {
		c.showError("Topic name cannot start with _")
	} else {
		err := c.CreateTopic(ctx, name)
		if err != nil {
			c.showError(status.Convert(err).Message())
		}
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
			// Check if we are still on the same topic
			if c.currentTopic == "" {
				return
			}
			// If message has topic name, verify it matches current topic
			if event.Message != nil && event.Message.TopicName != "" && event.Message.TopicName != c.currentTopic {
				return
			}

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
	fmt.Fprintf(c.messageView, "[\"user_%d\"][yellow]%s[\"\"][white]: %s\n", msg.UserId, userName, msg.Text)

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
