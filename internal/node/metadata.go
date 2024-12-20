package node

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
)

type Metadata struct {
	CurrentTerm uint64  `json:"current_term"`
	VotedFor    uint64  `json:"voted_for"`
	LogLength   int     `json:"log_length"`
	Log         []Entry `json:"log"`
}

func (n *Node) saveMetadata() error {
	msg := &Metadata{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.getVotedFor(n.id),
		LogLength:   len(n.log),
		Log:         n.log,
	}

	b, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("can't marshal json: %w", err)
	}

	if n.fd == nil {
		if err = n.initFileDesc(); err != nil {
			return fmt.Errorf("can't init file desc: %w", err)
		}
	}

	if _, err = n.fd.Seek(0, 0); err != nil {
		return fmt.Errorf("can't seek in file: %w", err)
	}

	w, err := n.fd.Write(b)
	if err != nil {
		return fmt.Errorf("can't write metadata to file: %w", err)
	}

	n.logger.Info().Msgf("wrote %d bytes of metadata to %s path", w, path.Join(n.metadataDir, n.getMDFileName()))
	return nil
}

func (n *Node) restoreMetadata() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.fd == nil {
		if err := n.initFileDesc(); err != nil {
			return fmt.Errorf("can't init file desc: %w", err)
		}
	}

	if _, err := n.fd.Seek(0, 0); err != nil {
		return fmt.Errorf("can't seek in file: %w", err)
	}

	resultBuf := make([]byte, 0)
	resultSizeBytes := 0
	for {
		buf := make([]byte, _chunkSizeBytes)
		n, err := n.fd.Read(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("can't read from file: %w", err)
		}
		resultBuf = append(resultBuf, buf...)
		resultSizeBytes += n
	}

	resultBuf = resultBuf[:resultSizeBytes]
	if len(resultBuf) == 0 {
		n.ensureLog()
		return nil
	}
	var md Metadata
	if err := json.Unmarshal(resultBuf, &md); err != nil {
		return fmt.Errorf("can't unmarshal json: %w", err)
	}
	n.currentTerm = md.CurrentTerm
	n.setVotedFor(n.id, md.VotedFor)
	copy(n.log, md.Log)

	n.logger.Debug().Msgf("restore log with len=%d", md.LogLength)
	n.ensureLog()

	return nil
}

func (n *Node) initFileDesc() error {
	if err := os.MkdirAll(n.metadataDir, 0755); err != nil {
		return fmt.Errorf("can't create dir %s: %w", n.metadataDir, err)
	}

	fd, err := os.OpenFile(path.Join(n.metadataDir, n.getMDFileName()), os.O_SYNC|os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		return fmt.Errorf("can't open file descriptor: %w", err)
	}
	n.fd = fd
	return nil
}

func (n *Node) getMDFileName() string {
	return fmt.Sprintf("md_%d.dat", n.id)
}

func (n *Node) ensureLog() {
	if len(n.log) == 0 {
		n.log = append(n.log, Entry{})
	}
}
