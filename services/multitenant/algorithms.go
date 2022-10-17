//go:generate mockgen -destination=../../mocks/services/multitenant/mock_tenantstats.go -package mock_tenantstats github.com/rudderlabs/rudder-server/services/multitenant Algorithm

package multitenant

import (
	"math"
	"time"

	"github.com/rudderlabs/rudder-server/services/metric"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type Algorithm interface {
	GetRouterPickupJobs(destType string, noOfWorkers int, routerTimeOut time.Duration, jobQueryBatchSize int, timeGained float64) map[string]int
}

func (t *Stats) GetRouterPickupJobs(destType string, noOfWorkers int, routerTimeOut time.Duration, jobQueryBatchSize int, timeGained float64) map[string]int {
	t.routerJobCountMutex.RLock()
	defer t.routerJobCountMutex.RUnlock()
	t.routerLatencyMutex.RLock()
	defer t.routerLatencyMutex.RUnlock()

	workspacesWithJobs := getWorkspacesWithPendingJobs(destType, t.routerTenantLatencyStat[destType])
	boostedRouterTimeOut := getBoostedRouterTimeOut(routerTimeOut, timeGained, noOfWorkers)
	// TODO: Also while allocating jobs to router workers, we need to assign so that sum of assigned jobs latency equals the timeout

	runningJobCount := jobQueryBatchSize
	runningTimeCounter := float64(noOfWorkers) * float64(boostedRouterTimeOut) / float64(time.Second)
	workspacePickUpCount := make(map[string]int)

	minLatency, maxLatency := getMinMaxWorkspaceLatency(workspacesWithJobs, t.routerTenantLatencyStat[destType])

	scores := t.getSortedWorkspaceScoreList(workspacesWithJobs, maxLatency, minLatency, t.routerTenantLatencyStat[destType], destType)
	// Latency sorted input rate pass
	for _, scoredWorkspace := range scores {
		workspaceKey := scoredWorkspace.workspaceId
		workspaceCountKey, ok := t.routerInputRates["rt"][workspaceKey]
		if ok {
			destTypeCount, ok := workspaceCountKey[destType]
			if ok {

				if runningJobCount <= 0 || runningTimeCounter <= 0 {
					// Adding BETA
					if metric.PendingEvents("rt", workspaceKey, destType).Value() > 0 {
						workspacePickUpCount[workspaceKey] = 1
					}
					continue
				}
				// TODO : Get rid of unReliableLatencyORInRate hack
				unReliableLatencyORInRate := false
				pendingEvents := metric.PendingEvents("rt", workspaceKey, destType).IntValue()
				if t.routerTenantLatencyStat[destType][workspaceKey].Value() != 0 {
					tmpPickCount := int(math.Min(destTypeCount.Value()*float64(routerTimeOut)/float64(time.Second), runningTimeCounter/(t.routerTenantLatencyStat[destType][workspaceKey].Value())))
					if tmpPickCount < 1 {
						tmpPickCount = 1 // Adding BETA
						pkgLogger.Debugf("[DRAIN DEBUG] %v  checking for high latency/low in rate workspace %v latency value %v in rate %v", destType, workspaceKey, t.routerTenantLatencyStat[destType][workspaceKey].Value(), destTypeCount.Value())
						unReliableLatencyORInRate = true
					}
					workspacePickUpCount[workspaceKey] = tmpPickCount
					if workspacePickUpCount[workspaceKey] > pendingEvents {
						workspacePickUpCount[workspaceKey] = misc.MaxInt(pendingEvents, 0)
					}
				} else {
					workspacePickUpCount[workspaceKey] = misc.MinInt(int(destTypeCount.Value()*float64(routerTimeOut)/float64(time.Second)), pendingEvents)
				}

				timeRequired := float64(workspacePickUpCount[workspaceKey]) * t.routerTenantLatencyStat[destType][workspaceKey].Value()
				if unReliableLatencyORInRate {
					timeRequired = 0
				}
				runningTimeCounter = runningTimeCounter - timeRequired
				workspacePickUpCount[workspaceKey] = misc.MinInt(workspacePickUpCount[workspaceKey], runningJobCount)
				runningJobCount = runningJobCount - workspacePickUpCount[workspaceKey]
				pkgLogger.Debugf("Time Calculated : %v , Remaining Time : %v , Workspace : %v ,runningJobCount : %v , moving_average_latency : %v, routerInRate : %v ,DestType : %v,InRateLoop ", timeRequired, runningTimeCounter, workspaceKey, runningJobCount, t.routerTenantLatencyStat[destType][workspaceKey].Value(), destTypeCount.Value(), destType)
			}
		}
	}

	// Sort by workspaces who can get to realtime quickly
	secondaryScores := t.getSortedWorkspaceSecondaryScoreList(workspacesWithJobs, workspacePickUpCount, destType, t.routerTenantLatencyStat[destType])
	for _, scoredWorkspace := range secondaryScores {
		workspaceKey := scoredWorkspace.workspaceId
		pendingEvents := metric.PendingEvents("rt", workspaceKey, destType).IntValue()
		if pendingEvents <= 0 {
			continue
		}
		// BETA already added in the above loop
		if runningJobCount <= 0 || runningTimeCounter <= 0 {
			break
		}

		pickUpCount := 0
		if t.routerTenantLatencyStat[destType][workspaceKey].Value() == 0 {
			pickUpCount = misc.MinInt(pendingEvents-workspacePickUpCount[workspaceKey], runningJobCount)
		} else {
			tmpCount := int(runningTimeCounter / t.routerTenantLatencyStat[destType][workspaceKey].Value())
			pickUpCount = misc.MinInt(misc.MinInt(tmpCount, runningJobCount), pendingEvents-workspacePickUpCount[workspaceKey])
		}
		workspacePickUpCount[workspaceKey] += pickUpCount
		runningJobCount = runningJobCount - pickUpCount
		runningTimeCounter = runningTimeCounter - float64(pickUpCount)*t.routerTenantLatencyStat[destType][workspaceKey].Value()

		pkgLogger.Debugf("Time Calculated : %v , Remaining Time : %v , Workspace : %v ,runningJobCount : %v , moving_average_latency : %v, pileUpCount : %v ,DestType : %v ,PileUpLoop ", float64(pickUpCount)*t.routerTenantLatencyStat[destType][workspaceKey].Value(), runningTimeCounter, workspaceKey, runningJobCount, t.routerTenantLatencyStat[destType][workspaceKey].Value(), pendingEvents, destType)
	}

	return workspacePickUpCount
}
