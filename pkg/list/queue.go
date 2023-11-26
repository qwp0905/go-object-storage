package list

type Queue[T any] []T

func NewQueue[T any]() *Queue[T] {
	return &Queue[T]{}
}

func (q *Queue[T]) Len() int {
	return len(*q)
}

func (q *Queue[T]) Push(item T) {
	*q = append(*q, item)
}

func (q *Queue[T]) Shift() T {
	item := (*q)[0]
	*q = (*q)[1:]
	return item
}

func (q *Queue[T]) Flush() []T {
	out := make([]T, q.Len())
	copy(out, *q)
	*q = Queue[T]{}
	return out
}

func (q *Queue[T]) Pop() T {
	item := (*q)[len(*q)-1]
	*q = (*q)[:len(*q)-1]
	return item
}

func (q *Queue[T]) Last() T {
	return (*q)[len(*q)-1]
}

func (q *Queue[T]) First() T {
	return (*q)[0]
}
