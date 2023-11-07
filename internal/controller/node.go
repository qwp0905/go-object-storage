package controller

type NodeController struct {
	*controllerImpl
}

func NewNodeController() *NodeController {
	controller := &NodeController{
		controllerImpl: New("/node"),
	}

	controller.router.Post("/register")
	return controller
}
