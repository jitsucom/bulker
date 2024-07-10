package bulkerlib

import (
	"fmt"
	"testing"
)

func TestBulkerState(t *testing.T) {
	st := State{}
	st.AddWarehouseState(WarehouseState{
		Name:            "first",
		TimeProcessedMs: 100,
	})
	st.AddWarehouseState(WarehouseState{
		Name:            "second",
		TimeProcessedMs: 100,
	})

	st2 := State{}
	st2.AddWarehouseState(WarehouseState{
		Name:            "first",
		TimeProcessedMs: 101,
	})
	st2.AddWarehouseState(WarehouseState{
		Name:            "second",
		TimeProcessedMs: 120,
	})
	st2.AddWarehouseState(WarehouseState{
		Name:            "third",
		TimeProcessedMs: 130,
	})

	st.Merge(st2)

	fmt.Println(st.PrintWarehouseState())
}
