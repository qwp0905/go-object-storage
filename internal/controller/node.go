package controller

type Node struct {
	*controllerImpl
}

func NewNode() *Node {
	controller := &Node{
		controllerImpl: New("/node"),
	}

	controller.router.Post("/register")
	return controller
}

func RegisterNode() {}
