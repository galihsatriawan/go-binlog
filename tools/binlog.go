package tools

import (
	"fmt"
	"log"
	"runtime/debug"
	"strings"

	"github.com/siddontang/go-mysql/canal"
)

type binlogHandler struct {
	canal.DummyEventHandler
	BinlogParser
}

func (h *binlogHandler) OnRow(e *canal.RowsEvent) error {
	defer func() {
		if r := recover(); r != nil {
			fmt.Print(r, " ", string(debug.Stack()))
		}
	}()
	fmt.Println("Handler", e)
	// base value for canal.DeleteAction or canal.InsertAction
	var n = 0
	var k = 1

	if e.Action == canal.UpdateAction {
		n = 1
		k = 2
	}

	for i := n; i < len(e.Rows); i += k {
		fmt.Println("looping handler")
		key := e.Table.Schema + "." + e.Table.Name
		comparation := User{}.SchemaName() + "." + User{}.TableName()
		comparation = strings.ToLower(comparation)
		switch key {
		case comparation:
			user := User{}
			h.GetBinLogData(&user, e, i)
			switch e.Action {
			case canal.UpdateAction:
				oldUser := User{}
				h.GetBinLogData(&oldUser, e, i-1)
				fmt.Printf("User %d name changed from %s to %s\n", user.Id, oldUser.Name, user.Name)
			case canal.InsertAction:
				fmt.Printf("User %d is created with name %s\n", user.Id, user.Name)
			case canal.DeleteAction:
				fmt.Printf("User %d is deleted with name %s\n", user.Id, user.Name)
			default:
				fmt.Printf("Unknown action")
			}
		}

	}
	return nil
}

func (h *binlogHandler) String() string {
	return "binlogHandler"
}

func binlogListener() {
	c, err := getDefaultCanal()
	if err == nil {

		coords, err := c.GetMasterPos()
		if err == nil {
			fmt.Println("Success")
			c.SetEventHandler(&binlogHandler{})
			c.RunFrom(coords)
		}
		// c.SetEventHandler(&binlogHandler{})
		// c.Run()
	}
	log.Fatal(err)
}

func BinLogListener() {
	binlogListener()
}
func getDefaultCanal() (*canal.Canal, error) {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", "127.0.0.1", 3306)
	cfg.User = "sh0brun"
	cfg.Password = "Sh0brun20@)"
	cfg.Flavor = "mysql"

	cfg.Dump.ExecutionPath = ""

	return canal.NewCanal(cfg)
}
