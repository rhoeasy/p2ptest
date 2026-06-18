package application

import (
	"io"
	"time"

	"p2ptest/internal/logger"
	"p2ptest/internal/notifier"
	pb "p2ptest/proto/p2p"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MessagingService 实现 gRPC Messaging 服务接口。
// 职责：处理业务消息的双向流。
type MessagingService struct {
	pb.UnimplementedMessagingServer
	selfInfo *pb.NodeInfo
	notifier *notifier.Notifier
}

func NewMessagingService(selfInfo *pb.NodeInfo, notifier *notifier.Notifier) *MessagingService {
	return &MessagingService{
		selfInfo: selfInfo,
		notifier: notifier,
	}
}

func (s *MessagingService) Stream(stream pb.Messaging_StreamServer) error {
	for {
		env, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			st, ok := status.FromError(err)
			if ok && st.Code() == codes.Canceled {
				logger.L().Debug("[messaging] stream canceled")
				return nil
			}
			logger.L().Warn("[messaging] stream recv error", zap.Error(err))
			return err
		}

		if env == nil {
			continue
		}

		s.handleEnvelope(stream, env)
	}
}

func (s *MessagingService) handleEnvelope(stream pb.Messaging_StreamServer, env *pb.Envelope) {
	switch payload := env.Payload.(type) {
	case *pb.Envelope_Text:
		logger.L().Info("[messaging] received text",
			zap.String("from", env.From.Name),
			zap.String("content", payload.Text.Content),
		)

		// Emit notification for received message
		if s.notifier != nil {
			s.notifier.Emit(notifier.NewMessageReceivedNotification(env.From.Name, payload.Text.Content))
		}

	case *pb.Envelope_Ping:
		logger.L().Debug("[messaging] received ping", zap.String("from", env.From.Name))

		pongEnv := &pb.Envelope{
			MsgId:     env.MsgId + "-pong",
			From:      s.selfInfo.Id,
			Timestamp: uint64(time.Now().UnixMilli()),
			Payload: &pb.Envelope_Pong{
				Pong: &pb.Pong{
					Nonce:         payload.Ping.Nonce,
					PingTimestamp: env.Timestamp,
				},
			},
		}
		if err := stream.Send(pongEnv); err != nil {
			logger.L().Warn("[messaging] send pong failed", zap.Error(err))
		}

	default:
		logger.L().Warn("[messaging] unknown payload type", zap.String("from", env.From.Name))
	}
}
