package gossip

import (
	"os"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func TestMain(m *testing.M) {
	if os.Getenv("PRETTY") == "1" {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}
	if os.Getenv("DEBUG") == "1" {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
		log.Debug().Msg("Debugging logging activated")
	}

	os.Exit(m.Run())
}

func TestGossipSingleNode(t *testing.T) {
	t.Log("Testing gossip single")
	_, err := NewGossipManager("testpart", "0.0.0.0", 0, []string{})
	if err != nil {
		t.Fatal(err)
	}
}

func TestGossipDualNode(t *testing.T) {
	t.Log("Testing gossip double")
	_, err := NewGossipManager("testpart", "0.0.0.0", 9900, []string{})
	if err != nil {
		t.Fatal(err)
	}
	t.Log("First node set")
	_, err = NewGossipManager("testpart2", "0.0.0.0", 9901, []string{"localhost:9900"})
	if err != nil {
		t.Fatal(err)
	}
	t.Log("Second node set")
}
