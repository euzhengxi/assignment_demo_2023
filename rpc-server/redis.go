package main

import (
	"context"
	"encoding/json"
	"github.com/redis/go-redis/v9"
)

type RedisClient struct {
	cli *redis.Client
}

func (c *RedisClient) InitialiseClient(ctx context.Context, address, password string) error {
	redisClient := redis.NewClient(&redis.Options{
		Addr:     address,
		Password: password, // no password set
		DB:       0,        // use default DB
	})

	// test connection
	if error := redisClient.Ping(ctx).Err(); error != nil {
		return error
	}

	c.cli = redisClient
	return nil
}

func (c *RedisClient) SaveMessage(ctx context.Context, roomID string, message *Message) error {
	// Store the message in json
	encodedMessage, error := json.Marshal(message)
	if error != nil {
		return error
	}

	member := &redis.Z{
		Score:  float64(message.Timestamp),
		Member: encodedMessage,
	}

	_, error = c.cli.ZAdd(ctx, roomID, *member).Result()
	if error != nil {
		return error
	}

	return nil
}

func (c *RedisClient) GetMessagesByRoomID(ctx context.Context, roomID string, start, end int64, reverse bool) ([]*Message, error) {
	var (
		rawMessages []string
		messages    []*Message
		err         error
	)

	//default is to start from the latest message
	rawMessages, err = c.cli.ZRevRange(ctx, roomID, start, end).Result()
	if !reverse {
		rawMessages, err = c.cli.ZRange(ctx, roomID, start, end).Result()
	}

	if err != nil {
		return nil, err
	}

	for _, msg := range rawMessages {
		decodedMessage := &Message{}
		err := json.Unmarshal([]byte(msg), decodedMessage)
		if err != nil {
			return nil, err
		}
		messages = append(messages, decodedMessage)
	}

	return messages, nil
}
