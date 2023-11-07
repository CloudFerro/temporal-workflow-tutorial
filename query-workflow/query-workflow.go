package main

import (
	"context"
	"fmt"
	"log"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
)

func main() {
	// Create a new Temporal client
	c, err := client.Dial(client.Options{})
	if err != nil {
		panic(err)
	}
	defer c.Close()

	query := ""

	// List the workflow executions
	executions, err := c.ListWorkflow(context.Background(), &workflowservice.ListWorkflowExecutionsRequest{Query: query})
	if err != nil {
		panic(err)
	}

	// Get the ID of the last workflow execution
	var workflowID string
	if executions.Size() > 0 {
		workflowID = executions.GetExecutions()[0].GetExecution().GetWorkflowId()
	}

	fmt.Println(workflowID)
	// Check the status of the last workflow execution.
	var queryResult string
	resp, err := c.QueryWorkflow(context.Background(), workflowID, "", "current_state")
	if err != nil {
		log.Fatalf("Unable to query workflow status: %v", err)
	}
	err = resp.Get(&queryResult)
	if err != nil {
		log.Fatalf("Unable to get query result: %v", err)
	}
	fmt.Printf("Workflow Status: %s\n", queryResult)

	var result int
	err = c.GetWorkflow(context.Background(), workflowID, "").Get(context.Background(), &result)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Workflow Result: %v\n", result)

	// Print the workflow status
}
