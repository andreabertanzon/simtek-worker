package worker

import (
	"context"
	"log"
	"simtek-worker/internal/queue"
	"time"

	"github.com/google/uuid"
)

type Worker interface {
	Work()
}

type InterventionWorker struct {
	logger *log.Logger
	queue  queue.QueueReader
	guid   string
	ctx    context.Context
}

func NewInterventionWorker(logger *log.Logger, ctx context.Context) *InterventionWorker {
	interventionWorker := &InterventionWorker{
		logger: logger,
		guid:   uuid.NewString(),
		ctx:    ctx,
	}

	return interventionWorker
}

func (w *InterventionWorker) Work() {
	log.Printf("Worker [%s] starting work at: %s", w.guid, time.Now())

	// TODO: connect to the queue listening everything
	ch := make(chan<- string)
	err := w.queue.Read(ch, w.ctx)
	if err != nil {
		w.logger.Println("Error reading from queue:\n", err)
	}

	// TODO: foreach message read it and start processing the json, dividing again the original json and the db entitieo

}
