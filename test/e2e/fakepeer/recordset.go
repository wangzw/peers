//go:build e2e

package main

import (
	"bytes"
	"fmt"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/wangzw/peers/internal/directory/directorypb"
)

type recordSet struct {
	mu   sync.Mutex
	recs []*directorypb.Record
}

func (s *recordSet) set(recs []*directorypb.Record) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recs = append([]*directorypb.Record(nil), recs...)
}

func (s *recordSet) all() []*directorypb.Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*directorypb.Record(nil), s.recs...)
}

// list skips a record whose raw body does not parse.
func (s *recordSet) list() *directorypb.RecordList {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := &directorypb.RecordList{Records: make([]*directorypb.RecordVersion, 0, len(s.recs))}
	for _, rec := range s.recs {
		body := new(directorypb.RecordBody)
		if err := proto.Unmarshal(rec.GetRaw(), body); err != nil {
			continue
		}
		out.Records = append(out.Records, &directorypb.RecordVersion{
			Id: &directorypb.RecordId{
				Publisher: append([]byte(nil), body.GetPublisher()...),
				Kind:      body.GetKind(),
				Object:    body.GetObject(),
			},
			Version: body.GetVersion(),
		})
	}
	return out
}

func (s *recordSet) answer(req *directorypb.RecordRequest) *directorypb.Records {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := &directorypb.Records{}
	for _, rec := range s.recs {
		body := new(directorypb.RecordBody)
		if err := proto.Unmarshal(rec.GetRaw(), body); err != nil {
			continue
		}
		for _, id := range req.GetRecords() {
			if id.GetKind() != body.GetKind() || id.GetObject() != body.GetObject() {
				continue
			}
			if bytes.Equal(id.GetPublisher(), body.GetPublisher()) {
				out.Records = append(out.Records, rec)
				break
			}
		}
	}
	return out
}

// marshal panics on an encoding failure, which is a programming mistake here.
func marshal(m proto.Message) []byte {
	b, err := proto.Marshal(m)
	if err != nil {
		panic(fmt.Sprintf("fakepeer: marshal %T: %v", m, err))
	}
	return b
}
