// internal/dataplane/storage.go
package dataplane

import (
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	pb "messageboard/proto"

	"google.golang.org/protobuf/proto"
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
		name TEXT NOT NULL UNIQUE,
		password_hash TEXT NOT NULL,
		salt TEXT NOT NULL
	);

	CREATE TABLE IF NOT EXISTS topics (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL UNIQUE
	);

	CREATE TABLE IF NOT EXISTS messages (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		topic_id INTEGER NOT NULL,
		user_id INTEGER NOT NULL,
		text TEXT NOT NULL,
		created_at INTEGER NOT NULL,
		likes INTEGER DEFAULT 0,
		FOREIGN KEY (topic_id) REFERENCES topics(id),
		FOREIGN KEY (user_id) REFERENCES messages(id),
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

	CREATE TABLE IF NOT EXISTS topic_participants (
		topic_id INTEGER NOT NULL,
		user_id INTEGER NOT NULL,
		PRIMARY KEY (topic_id, user_id),
		FOREIGN KEY (topic_id) REFERENCES topics(id),
		FOREIGN KEY (user_id) REFERENCES users(id)
	);

	CREATE TABLE IF NOT EXISTS sequence (
		id INTEGER PRIMARY KEY CHECK (id = 1),
		last_seq INTEGER DEFAULT 0,
		last_acked_seq INTEGER DEFAULT 0
	);

	INSERT OR IGNORE INTO sequence (id, last_seq, last_acked_seq) VALUES (1, 0, 0);

	CREATE TABLE IF NOT EXISTS operation_log (
		sequence_id INTEGER PRIMARY KEY,
		op_data BLOB NOT NULL
	);
	`

	_, err := s.db.Exec(schema)
	return err
}

func (s *Storage) AddTopicParticipant(topicID, userID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, err := s.db.Exec("INSERT OR IGNORE INTO topic_participants (topic_id, user_id) VALUES (?, ?)", topicID, userID)
	return err
}

func (s *Storage) RemoveTopicParticipant(topicID, userID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, err := s.db.Exec("DELETE FROM topic_participants WHERE topic_id = ? AND user_id = ?", topicID, userID)
	return err
}

func (s *Storage) GetTopicParticipants(topicID int64) ([]*pb.User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := s.db.Query(`
		SELECT u.id, u.name, u.password_hash, u.salt 
		FROM users u
		JOIN topic_participants tp ON u.id = tp.user_id
		WHERE tp.topic_id = ?
	`, topicID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var users []*pb.User
	for rows.Next() {
		var u pb.User
		if err := rows.Scan(&u.Id, &u.Name, &u.PasswordHash, &u.Salt); err != nil {
			return nil, err
		}
		users = append(users, &u)
	}

	return users, rows.Err()
}

func (s *Storage) CreateUser(name, password, salt string) (*pb.User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if user already exists
	var exists int
	err := s.db.QueryRow("SELECT COUNT(*) FROM users WHERE name = ?", name).Scan(&exists)
	if err != nil {
		return nil, err
	}
	if exists > 0 {
		return nil, fmt.Errorf("user with name '%s' already exists", name)
	}

	// If salt is not provided (e.g. from client), generate it
	if salt == "" {
		salt, err = generateSalt()
		if err != nil {
			return nil, err
		}
	}

	hash := hashPassword(password, salt)

	result, err := s.db.Exec("INSERT INTO users (name, password_hash, salt) VALUES (?, ?, ?)", name, hash, salt)
	if err != nil {
		return nil, err
	}

	id, err := result.LastInsertId()
	if err != nil {
		return nil, err
	}

	return &pb.User{Id: id, Name: name, PasswordHash: hash, Salt: salt}, nil
}

func (s *Storage) VerifyUser(name, password string) (*pb.User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var user pb.User
	var storedHash, salt string

	err := s.db.QueryRow("SELECT id, name, password_hash, salt FROM users WHERE name = ?", name).Scan(&user.Id, &user.Name, &storedHash, &salt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("invalid credentials")
		}
		return nil, err
	}

	if hashPassword(password, salt) != storedHash {
		return nil, fmt.Errorf("invalid credentials")
	}

	user.PasswordHash = storedHash
	user.Salt = salt
	return &user, nil
}

func (s *Storage) GetUserByName(name string) (*pb.User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var user pb.User
	err := s.db.QueryRow("SELECT id, name, password_hash, salt FROM users WHERE name = ?", name).Scan(&user.Id, &user.Name, &user.PasswordHash, &user.Salt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil // User not found
		}
		return nil, err
	}

	return &user, nil
}

func (s *Storage) GetUserById(id int64) (*pb.User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	log.Printf("Getting user by ID: %d", id)
	var user pb.User
	err := s.db.QueryRow("SELECT id, name, password_hash, salt FROM users WHERE id = ?", id).Scan(&user.Id, &user.Name, &user.PasswordHash, &user.Salt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil // User not found
		}
		return nil, err
	}

	log.Printf("Found user: %s", user.String())
	return &user, nil
}

func (s *Storage) CreateTopic(name string, userID int64) (*pb.Topic, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if topic already exists
	var exists int
	err := s.db.QueryRow("SELECT COUNT(*) FROM topics WHERE name = ?", name).Scan(&exists)
	if err != nil {
		return nil, err
	}
	if exists > 0 {
		return nil, fmt.Errorf("topic with name '%s' already exists", name)
	}

	result, err := s.db.Exec("INSERT INTO topics (name) VALUES (?)", name)
	if err != nil {
		return nil, err
	}

	id, err := result.LastInsertId()
	if err != nil {
		return nil, err
	}

	// Add creator as participant
	if userID != 0 {
		_, err = s.db.Exec("INSERT INTO topic_participants (topic_id, user_id) VALUES (?, ?)", id, userID)
		if err != nil {
			return nil, err
		}
	}

	return &pb.Topic{Id: id, Name: name}, nil
}

func (s *Storage) GetTopicByName(name string) (*pb.Topic, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var topic pb.Topic
	err := s.db.QueryRow("SELECT id, name FROM topics WHERE name = ?", name).Scan(&topic.Id, &topic.Name)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil // Topic not found
		}
		return nil, err
	}

	return &topic, nil
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

	// Check if already liked
	var liked int
	err = s.db.QueryRow("SELECT COUNT(*) FROM likes WHERE message_id = ? AND user_id = ?", messageID, userID).Scan(&liked)
	if err != nil {
		return nil, err
	}

	if liked > 0 {
		// Unlike
		_, err = s.db.Exec(
			"DELETE FROM likes WHERE message_id = ? AND user_id = ?",
			messageID, userID,
		)
		if err != nil {
			return nil, err
		}
	} else {
		// Like
		_, err = s.db.Exec(
			"INSERT INTO likes (topic_id, message_id, user_id) VALUES (?, ?, ?)",
			topicID, messageID, userID,
		)
		if err != nil {
			return nil, err
		}
	}

	// Update like count
	_, err = s.db.Exec(
		"UPDATE messages SET likes = (SELECT COUNT(*) FROM likes WHERE message_id = ?) WHERE id = ?",
		messageID, messageID,
	)
	if err != nil {
		return nil, err
	}

	return s.GetMessage(messageID, userID)
}

func (s *Storage) GetMessage(messageID int64, userID int64) (*pb.Message, error) {
	var msg pb.Message
	var createdAt int64

	err := s.db.QueryRow(
		"SELECT id, topic_id, user_id, text, created_at, likes FROM messages WHERE id = ?",
		messageID,
	).Scan(&msg.Id, &msg.TopicId, &msg.UserId, &msg.Text, &createdAt, &msg.Likes)

	if err != nil {
		return nil, err
	}

	// Check if liked by user
	if userID != 0 {
		var liked int
		err = s.db.QueryRow("SELECT COUNT(*) FROM likes WHERE message_id = ? AND user_id = ?", messageID, userID).Scan(&liked)
		if err != nil {
			return nil, err
		}
		msg.IsLiked = liked > 0
	}

	msg.CreatedAt = timestamppb.New(time.Unix(createdAt, 0))
	return &msg, nil
}

func (s *Storage) ListTopics(userID int64) ([]*pb.Topic, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var rows *sql.Rows
	var err error

	if userID == 0 {
		// Return empty list if no user ID provided, to enforce filtering
		return []*pb.Topic{}, nil
	} else {
		rows, err = s.db.Query(`
			SELECT t.id, t.name 
			FROM topics t
			JOIN topic_participants tp ON t.id = tp.topic_id
			WHERE tp.user_id = ?
			ORDER BY t.id
		`, userID)
	}

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

func (s *Storage) ListAllTopics() ([]*pb.Topic, error) {
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

func (s *Storage) ListJoinableTopics(userID int64) ([]*pb.Topic, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := s.db.Query(`
		SELECT t.id, t.name 
		FROM topics t
		WHERE t.id NOT IN (
			SELECT topic_id FROM topic_participants WHERE user_id = ?
		)
		ORDER BY t.id
	`, userID)
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

func (s *Storage) GetMessages(topicID, fromMessageID int64, limit int32, requestUserID int64) ([]*pb.Message, error) {
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

		// Check if liked by user
		if requestUserID != 0 {
			var liked int
			err = s.db.QueryRow("SELECT COUNT(*) FROM likes WHERE message_id = ? AND user_id = ?", msg.Id, requestUserID).Scan(&liked)
			if err != nil {
				return nil, err
			}
			msg.IsLiked = liked > 0
		}

		messages = append(messages, &msg)
	}

	return messages, rows.Err()
}

func (s *Storage) GetMessagesByUser(topicID int64, userName string, limit int32, requestUserID int64) ([]*pb.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Get user ID first
	user, err := s.GetUserByName(userName)
	if err != nil {
		return nil, err
	}
	if user == nil {
		return nil, fmt.Errorf("user not found")
	}

	rows, err := s.db.Query(
		"SELECT id, topic_id, user_id, text, created_at, likes FROM messages WHERE topic_id = ? AND user_id = ? ORDER BY id DESC LIMIT ?",
		topicID, user.Id, limit,
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

		// Check if liked by user
		if requestUserID != 0 {
			var liked int
			err = s.db.QueryRow("SELECT COUNT(*) FROM likes WHERE message_id = ? AND user_id = ?", msg.Id, requestUserID).Scan(&liked)
			if err != nil {
				return nil, err
			}
			msg.IsLiked = liked > 0
		}

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

func (s *Storage) UpdateLastAcked(seq int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, err := s.db.Exec("UPDATE sequence SET last_acked_seq = ? WHERE id = 1 AND last_acked_seq < ?", seq, seq)
	return err
}

func (s *Storage) GetLastAcked() (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var seq int64
	err := s.db.QueryRow("SELECT last_acked_seq FROM sequence WHERE id = 1").Scan(&seq)
	if err != nil {
		return 0, err
	}
	return seq, nil
}

func (s *Storage) GetSyncState() (int64, int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var lastSeq, lastAcked int64
	err := s.db.QueryRow("SELECT last_seq, last_acked_seq FROM sequence WHERE id = 1").Scan(&lastSeq, &lastAcked)
	if err != nil {
		return 0, 0, err
	}
	return lastSeq, lastAcked, nil
}

func (s *Storage) Close() error {
	return s.db.Close()
}

// Helper functions for password hashing
const pepper = "super-secret-pepper-value"

func generateSalt() (string, error) {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

func hashPassword(password, salt string) string {
	// Combine password, salt, and pepper
	input := password + salt + pepper
	hash := sha256.Sum256([]byte(input))
	return base64.StdEncoding.EncodeToString(hash[:])
}

func (s *Storage) LogOperation(op *pb.WriteOp) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Printf("Performing operation: %s", op)

	// This gets called when LogOperation is called from the head (sequence number for the passed operation is not generated yet)
	if op.Sequence == 0 {
		// Increment sequence
		if _, err := s.db.Exec("UPDATE sequence SET last_seq = last_seq + 1 WHERE id = 1"); err != nil {
			return 0, err
		}
		var seq int64
		if err := s.db.QueryRow("SELECT last_seq FROM sequence WHERE id = 1").Scan(&seq); err != nil {
			return 0, err
		}
		op.Sequence = seq
	} else {
		// When replicating operations, we already have the sequence number generated by Head node, so just copy it.
		// Check if already exists
		var exists int
		s.db.QueryRow("SELECT COUNT(*) FROM operation_log WHERE sequence_id = ?", op.Sequence).Scan(&exists)
		if exists > 0 {
			return op.Sequence, nil // Already logged, treat as success
		}

		// Check for gaps
		var lastSeq int64
		s.db.QueryRow("SELECT last_seq FROM sequence WHERE id = 1").Scan(&lastSeq)
		if op.Sequence > lastSeq+1 {
			log.Printf("ERROR: Sequence gap detected! Received %d, expected %d", op.Sequence, lastSeq+1)
		}

		// Update local sequence tracker
		if _, err := s.db.Exec("UPDATE sequence SET last_seq = ? WHERE id = 1 AND last_seq < ?", op.Sequence, op.Sequence); err != nil {
			return 0, err
		}
	}

	data, err := proto.Marshal(op)
	if err != nil {
		return 0, err
	}

	if _, err := s.db.Exec("INSERT INTO operation_log (sequence_id, op_data) VALUES (?, ?)", op.Sequence, data); err != nil {
		return 0, err
	}

	return op.Sequence, nil
}

func (s *Storage) GetOperations(fromSeq int64) ([]*pb.WriteOp, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := s.db.Query("SELECT op_data FROM operation_log WHERE sequence_id >= ? ORDER BY sequence_id ASC", fromSeq)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ops []*pb.WriteOp
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		var op pb.WriteOp
		if err := proto.Unmarshal(data, &op); err != nil {
			return nil, err
		}
		ops = append(ops, &op)
	}
	return ops, nil
}
