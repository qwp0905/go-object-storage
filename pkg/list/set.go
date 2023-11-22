package list

type Set[T comparable] map[T]struct{}

func (s Set[T]) Add(t T) {
	s[t] = struct{}{}
}

func (s Set[T]) Has(t T) bool {
	_, ok := s[t]
	return ok
}

func (s Set[T]) Del(t T) {
	delete(s, t)
}

func (s Set[T]) Union(t Set[T]) {
	for k := range t {
		s.Add(k)
	}
}

func (s Set[T]) Intersect(t Set[T]) {
	for k := range t {
		if !s.Has(k) {
			s.Del(k)
		}
	}
}

func (s Set[T]) Values() []T {
	out := make([]T, 0)
	for k := range s {
		out = append(out, k)
	}
	return out
}

func (s Set[T]) Len() int {
	return len(s)
}
