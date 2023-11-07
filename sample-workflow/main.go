package main

import (
	"fmt"
	"log"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const (
	taskQueue    = "catalogue-count-queue"
	activityName = "count_products_activity"
)

func getDateFromQuery(query string, reStr string) (time.Time, int, error) {
	re := regexp.MustCompile(reStr)
	match := re.FindStringSubmatch(query)
	if len(match) < 2 {
		return time.Time{}, -1, fmt.Errorf("could not find date in catalogueQuery: %s", query)
	}
	date_str := match[1]
	date_, err := time.Parse("2006-01-02T15:04:05.000", date_str)
	if err != nil {
		date_, err = time.Parse("2006-01-02T15:04:05", date_str)
		if err != nil {
			return time.Time{}, -1, err
		}
	}
	return date_, len(date_str), nil
}

// Workflow returns number of products from simple odata query
// e.q. "https://datahub.creodias.eu/odata/v1/Products?$filter=((ContentDate/Start ge 2023-09-01T00:00:00.000Z and ContentDate/Start lt 2023-09-30T23:59:59.999Z) and (Online eq true) and (((((Collection/Name eq 'SENTINEL-2'))))))&$expand=Attributes&$expand=Assets&$count=True&$orderby=ContentDate/Start asc"
// this workflow is executed from python script:
func CountProductsWorkflow(ctx workflow.Context, catalogueQuery string) (int, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ScheduleToCloseTimeout: 50 * time.Second,
		RetryPolicy:            &temporal.RetryPolicy{MaximumAttempts: 3},
	})

	currentState := "started"
	queryType := "current_state"
	err := workflow.SetQueryHandler(ctx, queryType, func() (string, error) {
		return currentState, nil
	})
	if err != nil {
		currentState = "failed to register query handler"
		return -1, err
	}

	startDate, startDateStrLen, err := getDateFromQuery(catalogueQuery, `Start ge (\S+)Z[ )]`)
	if err != nil {
		return -1, err
	}
	endDate, endDateStrLen, err := getDateFromQuery(catalogueQuery, `Start lt (\S+)Z[ )]`)
	if err != nil {
		return -1, err
	}

	fmt.Println(startDate)
	fmt.Println(endDate)

	delta := time.Hour * 24 * 5
	tmpEndDate := startDate
	total_count := 0
	numLoops := int(math.Floor(endDate.Sub(startDate).Seconds() / delta.Seconds()))
	if math.Mod(endDate.Sub(startDate).Seconds(), delta.Seconds()) != 0 {
		numLoops += 1
	}
	loop := 0
	var response int
	for startDate.Before(endDate) {
		loop += 1
		tmpEndDate = tmpEndDate.Add(delta)
		if endDate.Before(tmpEndDate) {
			tmpEndDate = endDate
		}
		currentState = "step " + strconv.Itoa(loop) + " of " + strconv.Itoa(numLoops)

		startDateIndex := strings.Index(catalogueQuery, "ContentDate/Start ge ") + len("ContentDate/Start ge ")
		startDateStr := catalogueQuery[startDateIndex : startDateIndex+startDateStrLen]
		newQuery := strings.ReplaceAll(catalogueQuery, startDateStr, startDate.Format("2006-01-02T15:04:05"))
		endDateIndex := strings.Index(newQuery, "ContentDate/Start lt ") + len("ContentDate/Start lt ")
		endDateStr := newQuery[endDateIndex : endDateIndex+endDateStrLen]
		newQuery = strings.ReplaceAll(newQuery, endDateStr, tmpEndDate.Format("2006-01-02T15:04:05"))
		err := workflow.ExecuteActivity(ctx, "count_products_activity", newQuery).Get(ctx, &response)
		workflow.GetLogger(ctx).Info("Counted " + strconv.Itoa(response) + " products")
		startDate = startDate.Add(delta)
		if err != nil {
			return -1, err
		}
		if err != nil {
			return -1, err
		}
		total_count += response
		time.Sleep(1 * time.Second)
	}
	return total_count, err
}

func main() {
	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatalf("Failed creating client: %v", err)
	}
	defer c.Close()

	w := worker.New(c, taskQueue, worker.Options{})
	w.RegisterWorkflowWithOptions(CountProductsWorkflow, workflow.RegisterOptions{Name: activityName})
	log.Printf("Starting worker (ctrl+c to exit)")
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("Worker failed to start: %v", err)
	}
}
