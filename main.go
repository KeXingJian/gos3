package main

import (
	"os"

	"github.com/kxj/gos3/internal/cli"
)

func main() {
	os.Exit(cli.Main(os.Args))
}
