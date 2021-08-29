package kvraft

import "sync"

type Pubsub struct {
    mu   sync.RWMutex
    subs map[string][]chan OpResult
    closed bool
}

func NewPubsub() *Pubsub {
    ps := &Pubsub{}
    ps.subs = make(map[string][]chan OpResult)
    return ps
}

func (ps *Pubsub) Subscribe(topic string) <-chan OpResult {
    ps.mu.Lock()
    defer ps.mu.Unlock()

    ch := make(chan OpResult, 1)
    ps.subs[topic] = append(ps.subs[topic], ch)
    return ch
}

func (ps *Pubsub) Unsubscribe(topic string, c <-chan OpResult) {
    ps.mu.Lock()
    defer ps.mu.Unlock()

    indexToRemove := -1
    for index, ch := range ps.subs[topic] {
        if c == ch {
            indexToRemove = index
            close(ch)
            break
        }
    }
    ps.subs[topic] = append(ps.subs[topic][:indexToRemove], ps.subs[topic][indexToRemove+1:]...)
}

func (ps *Pubsub) Publish(topic string, msg OpResult) {
    ps.mu.RLock()
    defer ps.mu.RUnlock()

    if ps.closed {
        return
    }

    for _, ch := range ps.subs[topic] {
        ch <- msg
    }
}

func (ps *Pubsub) Close() {
    ps.mu.Lock()
    defer ps.mu.Unlock()

    if !ps.closed {
        ps.closed = true
        for _, subs := range ps.subs {
            for _, ch := range subs {
                close(ch)
            }
        }
    }
}