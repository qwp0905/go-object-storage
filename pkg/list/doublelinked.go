package list

type DoubleLinked[T comparable] struct {
	root DoubleLinkedElement[T]
	len  int
}

type DoubleLinkedElement[T comparable] struct {
	list       *DoubleLinked[T]
	next, prev *DoubleLinkedElement[T]
	Value      T
}

func (e *DoubleLinkedElement[T]) GetPrev() *DoubleLinkedElement[T] {
	if p := e.prev; e.list != nil && p != &e.list.root {
		return p
	}
	return nil
}

func NewDoubleLinkedList[T comparable]() *DoubleLinked[T] {
	l := &DoubleLinked[T]{len: 0}
	l.root.next = &l.root
	l.root.prev = &l.root
	l.root.list = l
	return l
}

func NewDoubleLinkedListElement[T comparable](v T) *DoubleLinkedElement[T] {
	return &DoubleLinkedElement[T]{Value: v}
}

func (l *DoubleLinked[T]) insert(e, at *DoubleLinkedElement[T]) {
	e.prev = at
	e.next = at.next
	e.prev.next = e
	e.next.prev = e
	e.list = l
	l.len++
}

func (l *DoubleLinked[T]) move(e, at *DoubleLinkedElement[T]) {
	if e == at {
		return
	}
	e.prev.next = e.next
	e.next.prev = e.prev

	e.prev = at
	e.next = at.next
	e.prev.next = e
	e.next.prev = e
}

func (l *DoubleLinked[T]) PushBack(e *DoubleLinkedElement[T]) {
	l.insert(e, l.root.prev)
}

func (l *DoubleLinked[T]) MoveBack(e *DoubleLinkedElement[T]) {
	l.move(e, l.root.prev)
}

func (l *DoubleLinked[T]) First() *DoubleLinkedElement[T] {
	if l.len == 0 {
		return nil
	}
	return l.root.next
}

func (l *DoubleLinked[T]) Last() *DoubleLinkedElement[T] {
	if l.len == 0 {
		return nil
	}
	return l.root.prev
}

func (l *DoubleLinked[T]) Remove(e *DoubleLinkedElement[T]) {
	e.prev.next = e.next
	e.next.prev = e.prev
	e.next = nil // avoid memory leaks
	e.prev = nil // avoid memory leaks
	e.list = nil
	l.len--
}
