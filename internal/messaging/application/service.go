package application

import (
	"fmt"
	"io"

	"p2ptest/internal/logger"
	pb "p2ptest/proto/p2p"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MessagingService 实现 gRPC Messaging 服务接口。
// 职责：处理业务消息的双向流。
type MessagingService struct {
	pb.UnimplementedMessagingServer
}

func NewMessagingService() *MessagingService {
	return &MessagingService{}
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

		reply := &pb.Envelope{
			MsgId:     env.MsgId + "-ack",
			From:      &pb.NodeID{Name: "server", Uuid: "server"},
			Timestamp: env.Timestamp,
			Payload: &pb.Envelope_Text{
				Text: &pb.TextMessage{Content: fmt.Sprintf("received: %s", payload.Text.Content)},
			},
		}

		if err := stream.Send(reply); err != nil {
			logger.L().Error("[messaging] send reply failed", zap.Error(err))
		}

	case *pb.Envelope_Ping:
		logger.L().Debug("[messaging] received ping", zap.String("from", env.From.Name))

	default:
		logger.L().Warn("[messaging] unknown payload type", zap.String("from", env.From.Name))
	}
}
