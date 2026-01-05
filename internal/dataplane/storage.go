// internal/dataplane/storage.go
package dataplane

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	pb "messageboard/proto"

	"google.golang.org/protobuf/types/known/timestamppb"
	_ "modernc.org/sqlite"
)

type Storage struct {
	db             *sql.DB
	mu             sync.RWMutex
	sequenceNumber int64
}

func NewStorage(dbPath string) (*Storage, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	s := &Storage{db: db}
	if err := s.initSchema(); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Storage) initSchema() error {
	schema := `
	CREATE TABLE IF NOT EXISTS users (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL
	);

	CREATE TABLE IF NOT EXISTS topics (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL
	);

	CREATE TABLE IF NOT EXISTS messages (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		topic_id INTEGER NOT NULL,
		user_id INTEGER NOT NULL,
		text TEXT NOT NULL,
		created_at INTEGER NOT NULL,
		likes INTEGER DEFAULT 0,
		FOREIGN KEY (topic_id) REFERENCES topics(id),
		FOREIGN KEY (user_id) REFERENCES users(id)
	);

	CREATE TABLE IF NOT EXISTS likes (
		topic_id INTEGER NOT NULL,
		message_id INTEGER NOT NULL,
		user_id INTEGER NOT NULL,
		PRIMARY KEY (message_id, user_id),
		FOREIGN KEY (topic_id) REFERENCES topics(id),
		FOREIGN KEY (message_id) REFERENCES messages(id),
		FOREIGN KEY (user_id) REFERENCES users(id)
	);

	CREATE TABLE IF NOT EXISTS sequence (
		id INTEGER PRIMARY KEY CHECK (id = 1),
		last_seq INTEGER DEFAULT 0
	);

	INSERT OR IGNORE INTO sequence (id, last_seq) VALUES (1, 0);
	`

	_, err := s.db.Exec(schema)
	return err
}

func (s *Storage) CreateUser(name string) (*pb.User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	result, err := s.db.Exec("INSERT INTO users (name) VALUES (?)", name)
	if err != nil {
		return nil, err
	}

	id, err := result.LastInsertId()
	if err != nil {
		return nil, err
	}

	return &pb.User{Id: id, Name: name}, nil
}

func (s *Storage) CreateTopic(name string) (*pb.Topic, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	result, err := s.db.Exec("INSERT INTO topics (name) VALUES (?)", name)
	if err != nil {
		return nil, err
	}

	id, err := result.LastInsertId()
	if err != nil {
		return nil, err
	}

	return &pb.Topic{Id: id, Name: name}, nil
}

func (s *Storage) PostMessage(topicID, userID int64, text string) (*pb.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if user and topic exist
	var exists int
	err := s.db.QueryRow("SELECT COUNT(*) FROM users WHERE id = ?", userID).Scan(&exists)
	if err != nil || exists == 0 {
		return nil, fmt.Errorf("user does not exist")
	}

	err = s.db.QueryRow("SELECT COUNT(*) FROM topics WHERE id = ?", topicID).Scan(&exists)
	if err != nil || exists == 0 {
		return nil, fmt.Errorf("topic does not exist")
	}

	now := time.Now().Unix()
	result, err := s.db.Exec(
		"INSERT INTO messages (topic_id, user_id, text, created_at, likes) VALUES (?, ?, ?, ?, 0)",
		topicID, userID, text, now,
	)
	if err != nil {
		return nil, err
	}

	id, err := result.LastInsertId()
	if err != nil {
		return nil, err
	}

	return &pb.Message{
		Id:        id,
		TopicId:   topicID,
		UserId:    userID,
		Text:      text,
		CreatedAt: timestamppb.New(time.Unix(now, 0)),
		Likes:     0,
	}, nil
}

func (s *Storage) LikeMessage(topicID, messageID, userID int64) (*pb.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if message exists
	var exists int
	err := s.db.QueryRow("SELECT COUNT(*) FROM messages WHERE id = ? AND topic_id = ?", messageID, topicID).Scan(&exists)
	if err != nil || exists == 0 {
		return nil, fmt.Errorf("message does not exist")
	}

	// Insert like (ignore if already liked)
	_, err = s.db.Exec(
		"INSERT OR IGNORE INTO likes (topic_id, message_id, user_id) VALUES (?, ?, ?)",
		topicID, messageID, userID,
	)
	if err != nil {
		return nil, err
	}

	// Update like count
	_, err = s.db.Exec(
		"UPDATE messages SET likes = (SELECT COUNT(*) FROM likes WHERE message_id = ?) WHERE id = ?",
		messageID, messageID,
	)
	if err != nil {
		return nil, err
	}

	return s.GetMessage(messageID)
}

func (s *Storage) GetMessage(messageID int64) (*pb.Message, error) {
	var msg pb.Message
	var createdAt int64

	err := s.db.QueryRow(
		"SELECT id, topic_id, user_id, text, created_at, likes FROM messages WHERE id = ?",
		messageID,
	).Scan(&msg.Id, &msg.TopicId, &msg.UserId, &msg.Text, &createdAt, &msg.Likes)

	if err != nil {
		return nil, err
	}

	msg.CreatedAt = timestamppb.New(time.Unix(createdAt, 0))
	return &msg, nil
}

func (s *Storage) ListTopics() ([]*pb.Topic, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := s.db.Query("SELECT id, name FROM topics ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var topics []*pb.Topic
	for rows.Next() {
		var t pb.Topic
		if err := rows.Scan(&t.Id, &t.Name); err != nil {
			return nil, err
		}
		topics = append(topics, &t)
	}

	return topics, rows.Err()
}

func (s *Storage) GetMessages(topicID, fromMessageID int64, limit int32) ([]*pb.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := s.db.Query(
		"SELECT id, topic_id, user_id, text, created_at, likes FROM messages WHERE topic_id = ? AND id >= ? ORDER BY id LIMIT ?",
		topicID, fromMessageID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []*pb.Message
	for rows.Next() {
		var msg pb.Message
		var createdAt int64
		if err := rows.Scan(&msg.Id, &msg.TopicId, &msg.UserId, &msg.Text, &createdAt, &msg.Likes); err != nil {
			return nil, err
		}
		msg.CreatedAt = timestamppb.New(time.Unix(createdAt, 0))
		messages = append(messages, &msg)
	}

	return messages, rows.Err()
}

func (s *Storage) NextSequence() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.db.Exec("UPDATE sequence SET last_seq = last_seq + 1 WHERE id = 1")
	var seq int64
	s.db.QueryRow("SELECT last_seq FROM sequence WHERE id = 1").Scan(&seq)
	return seq
}

func (s *Storage) GetLastSequence() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var seq int64
	s.db.QueryRow("SELECT last_seq FROM sequence WHERE id = 1").Scan(&seq)
	return seq
}

func (s *Storage) Close() error {
	return s.db.Close()
}
