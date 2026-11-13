package main

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestTrajectoryFixtures(t *testing.T) {
	cases, err := loadFixtures(strings.NewReader(examples))
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			if _, err := runFixture(ctx, c, mockModel{}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTrajectoryChecksObservedArguments(t *testing.T) {
	cases, err := loadFixtures(strings.NewReader(examples))
	if err != nil {
		t.Fatal(err)
	}
	c := cases[0]
	c.Expect.Calls[0].Args["key"] = "wrong"
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	got, err := runFixture(ctx, c, mockModel{})
	if err == nil || !strings.Contains(err.Error(), "tool calls") || len(got.Calls) != 1 || got.Calls[0].Args["key"] != "oak" {
		t.Fatalf("a bad expectation must fail against the actual tool body: %+v, %v", got, err)
	}
}
